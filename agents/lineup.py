"""
lineup.py — enriched pre-match data for analyzer_v2.

Provides:
  get_recent_form(team_id, league_id, last_n)
      → exponentially-weighted attack / defence rates from last N matches

  get_h2h(home_id, away_id, last_n)
      → average goals per side in last N head-to-head meetings

  get_lineups_and_injuries(fixture_id)
      → starting-XI availability + injury/suspension penalty factor (0.75–1.0)

All API calls respect the free-tier limit (100 req/day) by caching in SQLite.
Cache TTL: 6 h for form/H2H, 1 h for lineups (re-check closer to kickoff).
"""

import logging
import math
import sqlite3
import time
from datetime import datetime
from typing import Optional

import requests

from agents.collector import AF_HEADERS
from config import API_FOOTBALL_URL, DB_PATH

logger = logging.getLogger(__name__)

FORM_TTL_H    = 6
LINEUP_TTL_H  = 1
FORM_LAST_N   = 6    # recent matches used for form
H2H_LAST_N    = 8    # H2H matches


# ── Cache helpers ──────────────────────────────────────────────────────────────

def _init_lineup_tables():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS form_cache (
            team_id INTEGER, league_id INTEGER, last_n INTEGER,
            home_attack REAL, home_conceded REAL,
            away_attack REAL, away_conceded REAL,
            form_str TEXT,
            cached_at TEXT,
            PRIMARY KEY (team_id, league_id, last_n)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS h2h_cache (
            home_id INTEGER, away_id INTEGER, last_n INTEGER,
            avg_home_goals REAL, avg_away_goals REAL,
            sample INTEGER,
            cached_at TEXT,
            PRIMARY KEY (home_id, away_id, last_n)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS lineup_cache (
            fixture_id INTEGER PRIMARY KEY,
            home_factor REAL, away_factor REAL,
            home_injuries INTEGER, away_injuries INTEGER,
            has_lineup INTEGER,
            cached_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def _age_hours(cached_at: str) -> float:
    return (datetime.utcnow() - datetime.fromisoformat(cached_at)).total_seconds() / 3600


# ── Recent form ────────────────────────────────────────────────────────────────

def get_recent_form(team_id: int, league_id: int, last_n: int = FORM_LAST_N) -> dict:
    """
    Return exponentially-weighted attack/defence rates from last N matches.
    Weights: w_k = exp(-0.35 * k), k=0 most recent → w=1.0, k=5 → w=0.17

    Returns:
        home_attack, home_conceded  (per home match)
        away_attack, away_conceded  (per away match)
        form_str                    e.g. "WDLWW"
    """
    _init_lineup_tables()

    # Check cache
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT home_attack, home_conceded, away_attack, away_conceded, form_str, cached_at "
        "FROM form_cache WHERE team_id=? AND league_id=? AND last_n=?",
        (team_id, league_id, last_n),
    )
    row = c.fetchone()
    conn.close()
    if row and _age_hours(row[5]) < FORM_TTL_H:
        return {
            "home_attack": row[0], "home_conceded": row[1],
            "away_attack": row[2], "away_conceded": row[3],
            "form_str":    row[4],
        }

    fallback = {
        "home_attack": 1.30, "home_conceded": 1.10,
        "away_attack": 1.00, "away_conceded": 1.30,
        "form_str": "?????",
    }

    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/fixtures",
            headers=AF_HEADERS,
            params={
                "team": team_id, "league": league_id,
                "season": datetime.utcnow().year,
                "last": last_n, "timezone": "UTC",
                "status": "FT",
            },
            timeout=10,
        )
        matches = resp.json().get("response", [])
        if not matches:
            return fallback

        # Sort newest first
        matches.sort(key=lambda x: x["fixture"]["date"], reverse=True)

        home_scored_w = home_conceded_w = away_scored_w = away_conceded_w = 0.0
        home_weight   = away_weight = 0.0
        form_chars = []

        for k, m in enumerate(matches):
            w = math.exp(-0.35 * k)
            home_id_m = m["teams"]["home"]["id"]
            score = m["score"]["fullTime"]
            hg = score.get("home") or 0
            ag = score.get("away") or 0

            if home_id_m == team_id:
                home_scored_w   += hg * w
                home_conceded_w += ag * w
                home_weight     += w
                if hg > ag:   form_chars.append("W")
                elif hg < ag: form_chars.append("L")
                else:         form_chars.append("D")
            else:
                away_scored_w   += ag * w
                away_conceded_w += hg * w
                away_weight     += w
                if ag > hg:   form_chars.append("W")
                elif ag < hg: form_chars.append("L")
                else:         form_chars.append("D")

        result = {
            "home_attack":   home_scored_w   / home_weight   if home_weight   else 1.30,
            "home_conceded": home_conceded_w  / home_weight   if home_weight   else 1.10,
            "away_attack":   away_scored_w    / away_weight   if away_weight   else 1.00,
            "away_conceded": away_conceded_w  / away_weight   if away_weight   else 1.30,
            "form_str":      "".join(form_chars[:5]) or "?????",
        }

        # Save to cache
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO form_cache
            (team_id, league_id, last_n,
             home_attack, home_conceded, away_attack, away_conceded,
             form_str, cached_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            team_id, league_id, last_n,
            result["home_attack"], result["home_conceded"],
            result["away_attack"], result["away_conceded"],
            result["form_str"], datetime.utcnow().isoformat(),
        ))
        conn.commit()
        conn.close()
        time.sleep(0.2)
        return result

    except Exception as e:
        logger.error(f"[Lineup] Recent form error team={team_id}: {e}")
        return fallback


# ── Head-to-head ───────────────────────────────────────────────────────────────

def get_h2h(home_id: int, away_id: int, last_n: int = H2H_LAST_N) -> dict:
    """
    Return average goals for each side from last N H2H meetings
    (from home_id's perspective as the home team).
    """
    _init_lineup_tables()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT avg_home_goals, avg_away_goals, sample, cached_at "
        "FROM h2h_cache WHERE home_id=? AND away_id=? AND last_n=?",
        (home_id, away_id, last_n),
    )
    row = c.fetchone()
    conn.close()
    if row and _age_hours(row[3]) < FORM_TTL_H:
        return {"avg_home_goals": row[0], "avg_away_goals": row[1], "sample": row[2]}

    fallback = {"avg_home_goals": 1.40, "avg_away_goals": 1.10, "sample": 0}

    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/fixtures/headtohead",
            headers=AF_HEADERS,
            params={"h2h": f"{home_id}-{away_id}", "last": last_n,
                    "status": "FT", "timezone": "UTC"},
            timeout=10,
        )
        matches = resp.json().get("response", [])
        if not matches:
            return fallback

        home_goals_total = away_goals_total = 0.0
        count = 0
        for m in matches:
            hid = m["teams"]["home"]["id"]
            score = m["score"]["fullTime"]
            hg = score.get("home") or 0
            ag = score.get("away") or 0
            # normalise to home_id always as "home"
            if hid == home_id:
                home_goals_total += hg
                away_goals_total += ag
            else:
                home_goals_total += ag
                away_goals_total += hg
            count += 1

        if count == 0:
            return fallback

        result = {
            "avg_home_goals": round(home_goals_total / count, 3),
            "avg_away_goals": round(away_goals_total / count, 3),
            "sample": count,
        }

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO h2h_cache
            (home_id, away_id, last_n, avg_home_goals, avg_away_goals, sample, cached_at)
            VALUES (?,?,?,?,?,?,?)
        """, (home_id, away_id, last_n,
              result["avg_home_goals"], result["avg_away_goals"],
              result["sample"], datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()
        time.sleep(0.2)
        return result

    except Exception as e:
        logger.error(f"[Lineup] H2H error {home_id}-{away_id}: {e}")
        return fallback


# ── Lineups & injuries ─────────────────────────────────────────────────────────

def get_lineups_and_injuries(fixture_id: int, home_id: int, away_id: int) -> dict:
    """
    Return:
        has_lineup    bool   — True if starting XI is published
        home_factor   float  — attack/defence multiplier for home (0.75–1.0)
        away_factor   float  — attack/defence multiplier for away (0.75–1.0)
        home_injuries int    — number of unavailable home players
        away_injuries int    — number of unavailable away players
    """
    _init_lineup_tables()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT home_factor, away_factor, home_injuries, away_injuries, has_lineup, cached_at "
        "FROM lineup_cache WHERE fixture_id=?",
        (fixture_id,),
    )
    row = c.fetchone()
    conn.close()
    if row and _age_hours(row[5]) < LINEUP_TTL_H:
        return {
            "home_factor":   row[0], "away_factor":   row[1],
            "home_injuries": row[2], "away_injuries": row[3],
            "has_lineup":    bool(row[4]),
        }

    result = {
        "home_factor": 1.0, "away_factor": 1.0,
        "home_injuries": 0, "away_injuries": 0,
        "has_lineup": False,
    }

    # ── Injuries / suspensions
    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/injuries",
            headers=AF_HEADERS,
            params={"fixture": fixture_id},
            timeout=10,
        )
        players = resp.json().get("response", [])
        home_inj = sum(1 for p in players if p["team"]["id"] == home_id)
        away_inj = sum(1 for p in players if p["team"]["id"] == away_id)
        result["home_injuries"] = home_inj
        result["away_injuries"] = away_inj
        # Each missing player reduces factor by 4%, floor 0.75
        result["home_factor"] = max(0.75, 1.0 - 0.04 * home_inj)
        result["away_factor"] = max(0.75, 1.0 - 0.04 * away_inj)
        time.sleep(0.2)
    except Exception as e:
        logger.error(f"[Lineup] Injuries error fixture={fixture_id}: {e}")

    # ── Lineups (only available ~60 min before kickoff)
    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/fixtures/lineups",
            headers=AF_HEADERS,
            params={"fixture": fixture_id},
            timeout=10,
        )
        lineups = resp.json().get("response", [])
        if len(lineups) >= 2:
            result["has_lineup"] = True
        time.sleep(0.2)
    except Exception as e:
        logger.error(f"[Lineup] Lineup error fixture={fixture_id}: {e}")

    # Cache
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO lineup_cache
        (fixture_id, home_factor, away_factor,
         home_injuries, away_injuries, has_lineup, cached_at)
        VALUES (?,?,?,?,?,?,?)
    """, (
        fixture_id,
        result["home_factor"], result["away_factor"],
        result["home_injuries"], result["away_injuries"],
        int(result["has_lineup"]),
        datetime.utcnow().isoformat(),
    ))
    conn.commit()
    conn.close()
    return result


# ── Form string display ────────────────────────────────────────────────────────

def form_emoji(form_str: str) -> str:
    """Convert 'WDLWW' → '🟢⬜🔴🟢🟢'"""
    mp = {"W": "🟢", "D": "⬜", "L": "🔴", "?": "❓"}
    return "".join(mp.get(ch, "❓") for ch in form_str)
