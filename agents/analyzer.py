import math
from agents.collector import get_team_stats, get_odds
from config import MIN_EDGE, MIN_ODDS, MAX_ODDS

# League average goals (used to normalise attack/defence strengths)
LEAGUE_AVG_HOME = 1.50
LEAGUE_AVG_AWAY = 1.20


def _poisson(lam: float, k: int) -> float:
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def match_probabilities(
    home_attack: float, home_defense: float,
    away_attack: float, away_defense: float,
) -> tuple[float, float, float]:
    """Return (P_home_win, P_draw, P_away_win) via Poisson model."""
    home_xg = max(home_attack * away_defense / LEAGUE_AVG_AWAY, 0.1)
    away_xg = max(away_attack * home_defense / LEAGUE_AVG_HOME, 0.1)

    home_win = draw = away_win = 0.0
    for i in range(9):
        for j in range(9):
            p = _poisson(home_xg, i) * _poisson(away_xg, j)
            if i > j:
                home_win += p
            elif i == j:
                draw += p
            else:
                away_win += p

    total = home_win + draw + away_win
    if total > 0:
        home_win /= total
        draw    /= total
        away_win /= total
    return home_win, draw, away_win


def _edge(model_prob: float, bookie_odds: float) -> float:
    if bookie_odds <= 0:
        return -1.0
    return model_prob - 1.0 / bookie_odds


def analyze_fixture(fixture: dict) -> list[dict]:
    """Return list of value bets for a single fixture."""
    home_stats = get_team_stats(fixture["home_id"], fixture["league_id"])
    away_stats = get_team_stats(fixture["away_id"], fixture["league_id"])
    odds = get_odds(fixture)

    if not odds:
        return []

    home_p, draw_p, away_p = match_probabilities(
        home_attack=home_stats["home_scored"],
        home_defense=home_stats["home_conceded"],
        away_attack=away_stats["away_scored"],
        away_defense=away_stats["away_conceded"],
    )

    candidates = [
        ("1X2", "Home Win", home_p, odds.get("home_win", 0)),
        ("1X2", "Draw",     draw_p, odds.get("draw",     0)),
        ("1X2", "Away Win", away_p, odds.get("away_win", 0)),
    ]

    value_bets = []
    for market, selection, model_prob, bookie_odds in candidates:
        if bookie_odds < MIN_ODDS or bookie_odds > MAX_ODDS:
            continue
        edge = _edge(model_prob, bookie_odds)
        if edge >= MIN_EDGE:
            value_bets.append({
                "match_id":   str(fixture["fixture_id"]),
                "home_team":  fixture["home_team"],
                "away_team":  fixture["away_team"],
                "league":     fixture["league_name"],
                "market":     market,
                "selection":  selection,
                "odds":       round(bookie_odds, 2),
                "model_prob": round(model_prob, 4),
                "edge":       round(edge, 4),
                "kickoff":    fixture["kickoff"],
            })
    return value_bets
