import requests
from datetime import datetime, timedelta
from config import (
    API_FOOTBALL_URL, API_FOOTBALL_KEY,
    ODDS_API_URL, ODDS_API_KEY,
    LEAGUE_IDS, WINDOW_HOURS,
)

AF_HEADERS = {"x-apisports-key": API_FOOTBALL_KEY}

# Map API-Football league IDs to The Odds API sport keys
SPORT_MAP = {
    39:  "soccer_epl",
    140: "soccer_spain_la_liga",
    78:  "soccer_germany_bundesliga",
    135: "soccer_italy_serie_a",
    61:  "soccer_france_ligue_one",
    88:  "soccer_netherlands_eredivisie",
    94:  "soccer_portugal_primeira_liga",
    203: "soccer_turkey_super_league",
}


def get_upcoming_fixtures(hours_from_now: float = 2.5, window: float = 4.0) -> list[dict]:
    """Return NS fixtures with kickoff in [now+hours_from_now, now+hours_from_now+window]."""
    now = datetime.utcnow()
    date_from = (now + timedelta(hours=max(hours_from_now - 0.5, 0))).strftime("%Y-%m-%d")
    date_to   = (now + timedelta(hours=hours_from_now + window)).strftime("%Y-%m-%d")

    fixtures = []
    for league_id in LEAGUE_IDS:
        params = {
            "league": league_id,
            "season": now.year,
            "from": date_from,
            "to": date_to,
            "status": "NS",
            "timezone": "UTC",
        }
        try:
            resp = requests.get(
                f"{API_FOOTBALL_URL}/fixtures",
                headers=AF_HEADERS, params=params, timeout=10,
            )
            for fix in resp.json().get("response", []):
                kickoff_str = fix["fixture"]["date"]
                # Parse UTC kickoff
                kickoff_dt = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
                kickoff_naive = kickoff_dt.replace(tzinfo=None)
                diff_h = (kickoff_naive - now).total_seconds() / 3600
                if diff_h < 0 or diff_h > hours_from_now + window:
                    continue
                fixtures.append({
                    "fixture_id": fix["fixture"]["id"],
                    "league_id": league_id,
                    "league_name": fix["league"]["name"],
                    "home_team": fix["teams"]["home"]["name"],
                    "away_team": fix["teams"]["away"]["name"],
                    "home_id": fix["teams"]["home"]["id"],
                    "away_id": fix["teams"]["away"]["id"],
                    "kickoff": kickoff_str,
                })
        except Exception as e:
            print(f"[Collector] Fixtures error league={league_id}: {e}")
    return fixtures


def get_team_stats(team_id: int, league_id: int) -> dict:
    """Fetch team season stats for the Poisson model."""
    params = {
        "team": team_id,
        "league": league_id,
        "season": datetime.utcnow().year,
    }
    fallback = {
        "home_scored": 1.30, "home_conceded": 1.10,
        "away_scored": 1.00, "away_conceded": 1.30,
    }
    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/teams/statistics",
            headers=AF_HEADERS, params=params, timeout=10,
        )
        stats = resp.json().get("response", {})
        played  = stats.get("fixtures", {}).get("played", {})
        goals   = stats.get("goals", {})
        ph = max(played.get("home", 0) or 1, 1)
        pa = max(played.get("away", 0) or 1, 1)
        gf = goals.get("for",     {}).get("total", {})
        ga = goals.get("against", {}).get("total", {})
        return {
            "home_scored":   (gf.get("home", 0) or 0) / ph,
            "home_conceded": (ga.get("home", 0) or 0) / ph,
            "away_scored":   (gf.get("away", 0) or 0) / pa,
            "away_conceded": (ga.get("away", 0) or 0) / pa,
        }
    except Exception as e:
        print(f"[Collector] Team stats error team={team_id}: {e}")
        return fallback


def get_odds(fixture: dict) -> dict:
    """Fetch 1X2 decimal odds from The Odds API for the fixture."""
    sport_key = SPORT_MAP.get(fixture["league_id"])
    if not sport_key:
        return {}

    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "eu",
        "markets": "h2h",
        "oddsFormat": "decimal",
    }
    try:
        resp = requests.get(
            f"{ODDS_API_URL}/sports/{sport_key}/odds",
            params=params, timeout=10,
        )
        home_key = fixture["home_team"].lower()
        away_key = fixture["away_team"].lower()

        for game in resp.json():
            g_home = game.get("home_team", "").lower()
            g_away = game.get("away_team", "").lower()
            # Token-based match: all words of length >= 4 must appear in opponent string
            def _name_match(a: str, b: str) -> bool:
                tokens = [t for t in a.split() if len(t) >= 4]
                return bool(tokens) and all(t in b for t in tokens)
            if _name_match(home_key, g_home) and _name_match(away_key, g_away):
                for bm in game.get("bookmakers", []):
                    for market in bm.get("markets", []):
                        if market["key"] == "h2h":
                            outcomes = {
                                o["name"].lower(): o["price"]
                                for o in market["outcomes"]
                            }
                            return {
                                "home_win": outcomes.get(g_home, 0),
                                "draw":     outcomes.get("draw", 0),
                                "away_win": outcomes.get(g_away, 0),
                            }
    except Exception as e:
        print(f"[Collector] Odds error: {e}")
    return {}
