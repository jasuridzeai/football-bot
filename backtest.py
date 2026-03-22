"""
Backtesting module for the Poisson value-bet model.

Data flow:
  1. Fetch finished fixtures per league from API-Football
  2. Cache team season stats in SQLite (TTL 24 h) to preserve API quota
  3. For each fixture run the Poisson model
  4. Simulate bookmaker odds: add Gaussian noise + apply overround margin
  5. Find value bets (edge > MIN_EDGE, odds in range)
  6. Simulate Kelly/4 staking starting from BANKROLL
  7. Report: accuracy, Brier score, ROI, calibration buckets

LIMITATION: team stats are taken from the FULL season, not the state at
match-day → slight look-ahead bias.  Acceptable for a first-pass validation.
Bookmaker odds are simulated (real historical odds require a paid tier).
"""

import random
import sqlite3
import time
import logging
from datetime import datetime
from typing import Optional

import requests

from agents.analyzer import match_probabilities
from agents.collector import AF_HEADERS
from config import (
    API_FOOTBALL_URL, LEAGUE_IDS, DB_PATH,
    MIN_EDGE, MIN_ODDS, MAX_ODDS,
    KELLY_FRACTION, MAX_BET_PCT, BANKROLL,
)

logger = logging.getLogger(__name__)

BACKTEST_MARGIN      = 0.06   # simulated bookmaker overround
BACKTEST_SIGMA       = 0.05   # Gaussian noise on bookie probs (market disagreement)
MAX_PER_LEAGUE       = 40     # recent finished fixtures per league
TEAM_STATS_TTL_HOURS = 24     # cache lifetime

LEAGUE_NAMES = {
    39:  "Premier League",
    140: "La Liga",
    78:  "Bundesliga",
    135: "Serie A",
    61:  "Ligue 1",
    88:  "Eredivisie",
    94:  "Primeira Liga",
    203: "Süper Lig",
}


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _init_tables():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS team_stats_cache (
            team_id   INTEGER,
            league_id INTEGER,
            season    INTEGER,
            home_scored   REAL, home_conceded REAL,
            away_scored   REAL, away_conceded REAL,
            cached_at TEXT,
            PRIMARY KEY (team_id, league_id, season)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS backtest_results (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            league_id  INTEGER,
            fixture_id INTEGER,
            home_team  TEXT,
            away_team  TEXT,
            kickoff    TEXT,
            p_home REAL, p_draw REAL, p_away REAL,
            sim_odds_home REAL, sim_odds_draw REAL, sim_odds_away REAL,
            actual_result  TEXT,
            bet_selection  TEXT,
            bet_odds  REAL,
            bet_stake REAL,
            bet_edge  REAL,
            profit    REAL,
            ran_at    TEXT
        )
    """)
    conn.commit()
    conn.close()


def _get_cached_stats(team_id: int, league_id: int, season: int) -> Optional[dict]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT home_scored, home_conceded, away_scored, away_conceded, cached_at "
        "FROM team_stats_cache WHERE team_id=? AND league_id=? AND season=?",
        (team_id, league_id, season),
    )
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    age_h = (datetime.utcnow() - datetime.fromisoformat(row[4])).total_seconds() / 3600
    if age_h > TEAM_STATS_TTL_HOURS:
        return None
    return dict(zip(
        ["home_scored", "home_conceded", "away_scored", "away_conceded"], row
    ))


def _save_cached_stats(team_id: int, league_id: int, season: int, stats: dict):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO team_stats_cache
        (team_id, league_id, season,
         home_scored, home_conceded, away_scored, away_conceded, cached_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        team_id, league_id, season,
        stats["home_scored"], stats["home_conceded"],
        stats["away_scored"], stats["away_conceded"],
        datetime.utcnow().isoformat(),
    ))
    conn.commit()
    conn.close()


# ── API helpers ────────────────────────────────────────────────────────────────

def _fetch_finished(league_id: int, season: int, limit: int) -> list:
    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/fixtures",
            headers=AF_HEADERS,
            params={"league": league_id, "season": season,
                    "status": "FT", "timezone": "UTC"},
            timeout=15,
        )
        all_f = resp.json().get("response", [])
        all_f.sort(key=lambda x: x["fixture"]["date"], reverse=True)
        return all_f[:limit]
    except Exception as e:
        logger.error(f"[Backtest] fixtures league={league_id}: {e}")
        return []


def _fetch_team_stats(team_id: int, league_id: int, season: int) -> dict:
    cached = _get_cached_stats(team_id, league_id, season)
    if cached:
        return cached
    fallback = {"home_scored": 1.30, "home_conceded": 1.10,
                "away_scored": 1.00, "away_conceded": 1.30}
    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/teams/statistics",
            headers=AF_HEADERS,
            params={"team": team_id, "league": league_id, "season": season},
            timeout=10,
        )
        s = resp.json().get("response", {})
        played = s.get("fixtures", {}).get("played", {})
        goals  = s.get("goals", {})
        ph = max(played.get("home", 0) or 1, 1)
        pa = max(played.get("away", 0) or 1, 1)
        gf = goals.get("for",     {}).get("total", {})
        ga = goals.get("against", {}).get("total", {})
        result = {
            "home_scored":   (gf.get("home", 0) or 0) / ph,
            "home_conceded": (ga.get("home", 0) or 0) / ph,
            "away_scored":   (gf.get("away", 0) or 0) / pa,
            "away_conceded": (ga.get("away", 0) or 0) / pa,
        }
        _save_cached_stats(team_id, league_id, season, result)
        time.sleep(0.25)
        return result
    except Exception as e:
        logger.error(f"[Backtest] team stats team={team_id}: {e}")
        return fallback


# ── Simulation ─────────────────────────────────────────────────────────────────

def _sim_odds(
    p_home: float, p_draw: float, p_away: float,
    margin: float, rng: random.Random,
) -> tuple[float, float, float]:
    """
    Simulate bookie odds:
    1. Add Gaussian noise (bookie has a different model → market disagreement)
    2. Apply overround so sum of implied probs = 1 + margin
    """
    noisy = [
        max(p_home + rng.gauss(0, BACKTEST_SIGMA), 0.01),
        max(p_draw + rng.gauss(0, BACKTEST_SIGMA), 0.01),
        max(p_away + rng.gauss(0, BACKTEST_SIGMA), 0.01),
    ]
    total = sum(noisy)
    normed = [x / total for x in noisy]
    odds = tuple(round(1.0 / (p * (1 + margin)), 2) for p in normed)
    return odds


def _actual_result(fix: dict) -> Optional[str]:
    score = fix.get("score", {}).get("fullTime", {})
    hg, ag = score.get("home"), score.get("away")
    if hg is None or ag is None:
        return None
    return "H" if hg > ag else ("D" if hg == ag else "A")


def _kelly(model_prob: float, odds: float, bankroll: float) -> float:
    b = odds - 1.0
    q = 1.0 - model_prob
    raw = max((model_prob * b - q) / b, 0.0)
    return round(min(raw * KELLY_FRACTION * bankroll, bankroll * MAX_BET_PCT), 2)


# ── Core backtest ──────────────────────────────────────────────────────────────

def run_backtest(
    season: Optional[int] = None,
    max_per_league: int = MAX_PER_LEAGUE,
    margin: float = BACKTEST_MARGIN,
) -> dict:
    _init_tables()
    season  = season or datetime.utcnow().year
    ran_at  = datetime.utcnow().isoformat()
    rng     = random.Random(42)
    rows    = []
    api_calls = 0

    for league_id in LEAGUE_IDS:
        lname = LEAGUE_NAMES.get(league_id, str(league_id))
        logger.info(f"[Backtest] {lname} — fetching finished fixtures...")
        fixtures = _fetch_finished(league_id, season, max_per_league)
        api_calls += 1
        if not fixtures:
            logger.warning(f"[Backtest] {lname} — no data")
            continue
        logger.info(f"[Backtest] {lname} — {len(fixtures)} fixtures")

        # Prefetch & cache team stats (one call per unique team)
        team_ids = {
            fix["teams"]["home"]["id"] for fix in fixtures
        } | {fix["teams"]["away"]["id"] for fix in fixtures}
        stats_cache: dict[int, dict] = {}
        for tid in team_ids:
            stats_cache[tid] = _fetch_team_stats(tid, league_id, season)
            api_calls += 1

        for fix in fixtures:
            result_code = _actual_result(fix)
            if result_code is None:
                continue

            home_id   = fix["teams"]["home"]["id"]
            away_id   = fix["teams"]["away"]["id"]
            hs        = stats_cache.get(home_id, {})
            as_       = stats_cache.get(away_id, {})

            p_home, p_draw, p_away = match_probabilities(
                home_attack=hs.get("home_scored",   1.30),
                home_defense=hs.get("home_conceded", 1.10),
                away_attack=as_.get("away_scored",   1.00),
                away_defense=as_.get("away_conceded", 1.30),
            )

            sim_h, sim_d, sim_a = _sim_odds(p_home, p_draw, p_away, margin, rng)

            # Find best value bet for this fixture
            candidates = [
                ("Home Win", "H", p_home, sim_h),
                ("Draw",     "D", p_draw, sim_d),
                ("Away Win", "A", p_away, sim_a),
            ]
            best = None
            best_edge = MIN_EDGE - 1e-6
            for sel, code, prob, odds in candidates:
                if odds < MIN_ODDS or odds > MAX_ODDS:
                    continue
                edge = prob - 1.0 / odds
                if edge > best_edge:
                    best_edge = edge
                    best = (sel, code, prob, odds, edge)

            bet_sel = bet_odds = bet_stake = bet_edge = profit = None
            if best:
                sel, code, prob, odds, edge = best
                stake = _kelly(prob, odds, BANKROLL)
                if stake > 0:
                    bet_sel   = sel
                    bet_odds  = odds
                    bet_stake = stake
                    bet_edge  = round(edge, 4)
                    profit    = round(stake * (odds - 1), 2) \
                                if result_code == code else -stake

            rows.append({
                "league_id":     league_id,
                "fixture_id":    fix["fixture"]["id"],
                "home_team":     fix["teams"]["home"]["name"],
                "away_team":     fix["teams"]["away"]["name"],
                "kickoff":       fix["fixture"]["date"],
                "p_home":        round(p_home, 4),
                "p_draw":        round(p_draw, 4),
                "p_away":        round(p_away, 4),
                "sim_odds_home": sim_h,
                "sim_odds_draw": sim_d,
                "sim_odds_away": sim_a,
                "actual_result": result_code,
                "bet_selection": bet_sel,
                "bet_odds":      bet_odds,
                "bet_stake":     bet_stake,
                "bet_edge":      bet_edge,
                "profit":        profit,
                "ran_at":        ran_at,
            })

        time.sleep(1)  # be kind to the API between leagues

    _save_results(rows)
    return _compute_summary(rows, margin, season, api_calls)


def _save_results(rows: list[dict]):
    if not rows:
        return
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for r in rows:
        c.execute("""
            INSERT INTO backtest_results
            (league_id, fixture_id, home_team, away_team, kickoff,
             p_home, p_draw, p_away,
             sim_odds_home, sim_odds_draw, sim_odds_away,
             actual_result, bet_selection, bet_odds, bet_stake, bet_edge,
             profit, ran_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            r["league_id"], r["fixture_id"], r["home_team"], r["away_team"],
            r["kickoff"], r["p_home"], r["p_draw"], r["p_away"],
            r["sim_odds_home"], r["sim_odds_draw"], r["sim_odds_away"],
            r["actual_result"], r["bet_selection"], r["bet_odds"],
            r["bet_stake"], r["bet_edge"], r["profit"], r["ran_at"],
        ))
    conn.commit()
    conn.close()


def _compute_summary(rows: list, margin: float, season: int, api_calls: int) -> dict:
    n = len(rows)
    if n == 0:
        return {"error": "Нет данных — проверь API-ключ или сезон"}

    # Model accuracy (argmax prediction vs actual)
    correct = sum(
        1 for r in rows
        if max(("H", r["p_home"]), ("D", r["p_draw"]), ("A", r["p_away"]),
               key=lambda x: x[1])[0] == r["actual_result"]
    )
    accuracy = correct / n

    # Brier score
    brier = sum(
        (r["p_home"] - (1 if r["actual_result"] == "H" else 0)) ** 2 +
        (r["p_draw"] - (1 if r["actual_result"] == "D" else 0)) ** 2 +
        (r["p_away"] - (1 if r["actual_result"] == "A" else 0)) ** 2
        for r in rows
    ) / (n * 3)

    # Betting P&L
    bets         = [r for r in rows if r["bet_selection"] and r["profit"] is not None]
    total_bets   = len(bets)
    wins         = sum(1 for b in bets if b["profit"] > 0)
    total_profit = sum(b["profit"] for b in bets)
    total_staked = sum(b["bet_stake"] for b in bets)
    roi          = total_profit / total_staked * 100 if total_staked else 0
    win_rate     = wins / total_bets if total_bets else 0
    avg_edge     = sum(b["bet_edge"] for b in bets) / total_bets if total_bets else 0
    avg_odds     = sum(b["bet_odds"] for b in bets) / total_bets if total_bets else 0

    # Per-league breakdown
    by_league: dict[int, dict] = {}
    for r in rows:
        lid = r["league_id"]
        if lid not in by_league:
            by_league[lid] = {"fixtures": 0, "bets": 0, "profit": 0.0, "staked": 0.0}
        by_league[lid]["fixtures"] += 1
        if r["bet_selection"] and r["profit"] is not None:
            by_league[lid]["bets"]   += 1
            by_league[lid]["profit"] += r["profit"]
            by_league[lid]["staked"] += r["bet_stake"]

    league_summary = []
    for lid, d in by_league.items():
        roi_l = d["profit"] / d["staked"] * 100 if d["staked"] else 0
        league_summary.append({
            "name":     LEAGUE_NAMES.get(lid, str(lid)),
            "fixtures": d["fixtures"],
            "bets":     d["bets"],
            "profit":   round(d["profit"], 2),
            "roi":      round(roi_l, 1),
        })
    league_summary.sort(key=lambda x: x["roi"], reverse=True)

    # Calibration buckets
    calibration = []
    for lo in [i / 10 for i in range(10)]:
        hi = lo + 0.1
        bucket = []
        for r in rows:
            for outcome, key in [("H", "p_home"), ("D", "p_draw"), ("A", "p_away")]:
                p = r[key]
                if lo <= p < hi:
                    bucket.append(1.0 if r["actual_result"] == outcome else 0.0)
        if len(bucket) >= 10:
            calibration.append({
                "range":     f"{int(lo*100)}-{int(hi*100)}%",
                "predicted": round((lo + hi) / 2 * 100, 0),
                "actual":    round(sum(bucket) / len(bucket) * 100, 1),
                "n":         len(bucket),
            })

    return {
        "season":          season,
        "total_fixtures":  n,
        "accuracy":        round(accuracy * 100, 1),
        "brier_score":     round(brier, 4),
        "brier_baseline":  0.222,   # random model benchmark for 3-outcome
        "total_bets":      total_bets,
        "wins":            wins,
        "win_rate":        round(win_rate * 100, 1),
        "total_profit":    round(total_profit, 2),
        "total_staked":    round(total_staked, 2),
        "roi":             round(roi, 1),
        "avg_edge":        round(avg_edge * 100, 1),
        "avg_odds":        round(avg_odds, 2),
        "margin_simulated": margin,
        "api_calls":       api_calls,
        "by_league":       league_summary,
        "calibration":     calibration,
    }


# ── Formatting ─────────────────────────────────────────────────────────────────

def format_backtest_report(s: dict) -> str:
    if "error" in s:
        return f"❌ Бэктест: {s['error']}"

    verdict_brier = (
        "✅ лучше рандома" if s["brier_score"] < s["brier_baseline"]
        else "⚠️ хуже рандома"
    )
    roi_icon = "✅" if s["roi"] > 0 else "❌"

    lines = [
        f"<b>📊 Бэктест {s['season']} — {s['total_fixtures']} матчей</b>",
        f"<i>(симулированная маржа букмекера: {int(s['margin_simulated']*100)}%)</i>\n",

        "<b>— Модель —</b>",
        f"Точность (argmax): <b>{s['accuracy']}%</b>",
        f"Brier score: <b>{s['brier_score']}</b> {verdict_brier} (baseline {s['brier_baseline']})\n",

        "<b>— Ставки (Kelly/4) —</b>",
        f"Всего ставок:  {s['total_bets']}",
        f"Win rate:      <b>{s['win_rate']}%</b>",
        f"Сред. edge:    {s['avg_edge']}%",
        f"Сред. коэфф:  {s['avg_odds']}",
        f"Поставлено:    {s['total_staked']:.2f}€",
        f"Прибыль:       <b>{s['total_profit']:+.2f}€</b>",
        f"ROI:           {roi_icon} <b>{s['roi']:+.1f}%</b>\n",

        "<b>— По лигам —</b>",
    ]
    for lg in s["by_league"]:
        icon = "🟢" if lg["roi"] > 0 else "🔴"
        lines.append(
            f"{icon} {lg['name']}: {lg['bets']} ставок, "
            f"{lg['profit']:+.2f}€, ROI {lg['roi']:+.1f}%"
        )

    if s["calibration"]:
        lines.append("\n<b>— Калибровка (предсказано → факт) —</b>")
        for b in s["calibration"]:
            bar_pred = "█" * int(b["predicted"] / 5)
            bar_act  = "█" * int(b["actual"]    / 5)
            lines.append(
                f"{b['range']:>8s} | pred {b['predicted']:>4.0f}% {bar_pred}\n"
                f"         | fact {b['actual']:>4.1f}% {bar_act}  (n={b['n']})"
            )

    lines += [
        f"\n<i>API вызовов: {s['api_calls']}</i>",
        "<i>⚠️ Коэффициенты симулированы — не реальный P&L</i>",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    season = int(sys.argv[1]) if len(sys.argv) > 1 else None
    result = run_backtest(season=season)
    print(format_backtest_report(result))
