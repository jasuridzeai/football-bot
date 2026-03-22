"""
analyzer_v2.py — Enhanced Poisson model with:

  1. Recent-form xG  (last 6 matches, exponential recency weights)
     replaces season-average stats from analyzer.py
  2. H2H adjustment  (25 % blend of H2H goal rates into form rates)
  3. Dixon-Coles correction  (ρ = -0.13)
     fixes systematic under-estimation of 0-0 / 1-0 / 0-1 / 1-1
  4. Lineup / injury penalty  (−4 % per missing player, floor 0.75)

Output format is identical to analyzer.py so it is a drop-in replacement.
Extra fields added to each bet dict:
  model_version  "v2"
  home_form      e.g. "WDLWW"
  away_form      e.g. "LWWDL"
  h2h_sample     number of H2H matches used
  home_injuries  int
  away_injuries  int
  dc_applied     bool
"""

import math
import logging
from agents.collector import get_odds
from agents.lineup import get_recent_form, get_h2h, get_lineups_and_injuries, form_emoji
from config import MIN_EDGE, MIN_ODDS, MAX_ODDS

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
LEAGUE_AVG_HOME = 1.50   # used only as normalisation baseline
LEAGUE_AVG_AWAY = 1.20

H2H_WEIGHT  = 0.25       # blend: final = (1-H2H_WEIGHT)*form + H2H_WEIGHT*h2h
DC_RHO      = -0.13      # Dixon-Coles ρ parameter
MAX_GOALS   = 9          # summation range for Poisson grid


# ── Poisson helpers ────────────────────────────────────────────────────────────

def _poisson(lam: float, k: int) -> float:
    return math.exp(-lam) * (lam ** k) / math.factorial(k)


def _dc_tau(i: int, j: int, lam_h: float, lam_a: float, rho: float) -> float:
    """Dixon-Coles correction factor for low-scoring cells."""
    if i == 0 and j == 0:
        return 1.0 - lam_h * lam_a * rho
    if i == 0 and j == 1:
        return 1.0 + lam_h * rho
    if i == 1 and j == 0:
        return 1.0 + lam_a * rho
    if i == 1 and j == 1:
        return 1.0 - rho
    return 1.0


def match_probabilities_v2(
    home_xg: float,
    away_xg: float,
    use_dc: bool = True,
) -> tuple[float, float, float]:
    """
    Return (P_home_win, P_draw, P_away_win) using Poisson + optional DC correction.
    """
    home_xg = max(home_xg, 0.10)
    away_xg = max(away_xg, 0.10)

    home_win = draw = away_win = 0.0

    for i in range(MAX_GOALS):
        for j in range(MAX_GOALS):
            p = _poisson(home_xg, i) * _poisson(away_xg, j)
            if use_dc:
                p *= _dc_tau(i, j, home_xg, away_xg, DC_RHO)
            if i > j:
                home_win += p
            elif i == j:
                draw += p
            else:
                away_win += p

    total = home_win + draw + away_win
    if total > 0:
        home_win /= total
        draw     /= total
        away_win /= total
    return home_win, draw, away_win


# ── xG construction ────────────────────────────────────────────────────────────

def _build_xg(
    home_form:  dict,
    away_form:  dict,
    h2h:        dict,
    lu:         dict,
) -> tuple[float, float]:
    """
    Compute expected goals for home and away.

    Pipeline:
      1. Form-based rates (home team attacking at home, away team defending away)
      2. Blend with H2H historical rates
      3. Apply injury/lineup penalty
    """
    # Form-based xG (same Dixon-Coles normalisation as v1)
    form_home_xg = home_form["home_attack"] * away_form["away_conceded"] / LEAGUE_AVG_AWAY
    form_away_xg = away_form["away_attack"] * home_form["home_conceded"] / LEAGUE_AVG_HOME

    # H2H-based xG
    h2h_home_xg = h2h["avg_home_goals"]
    h2h_away_xg = h2h["avg_away_goals"]

    # Blend — weight H2H less when sample is small
    sample   = min(h2h.get("sample", 0), H2H_LAST_N := 8)
    h2h_w    = H2H_WEIGHT * (sample / 8)          # scale down if < 8 H2H matches
    form_w   = 1.0 - h2h_w

    home_xg = form_w * form_home_xg + h2h_w * h2h_home_xg
    away_xg = form_w * form_away_xg + h2h_w * h2h_away_xg

    # Lineup / injury penalty
    home_xg *= lu["home_factor"]
    away_xg *= lu["away_factor"]
    # Injuries also affect the defence: more injuries → more goals conceded
    # (apply reciprocal factor to the opponent's xG)
    home_xg *= (2.0 - lu["away_factor"])   # away defence weakened → home scores more
    away_xg *= (2.0 - lu["home_factor"])   # home defence weakened → away scores more

    # Clip to sensible range
    home_xg = max(min(home_xg, 5.0), 0.10)
    away_xg = max(min(away_xg, 5.0), 0.10)

    return round(home_xg, 3), round(away_xg, 3)


def _edge(model_prob: float, bookie_odds: float) -> float:
    if bookie_odds <= 0:
        return -1.0
    return model_prob - 1.0 / bookie_odds


# ── Main entry point ───────────────────────────────────────────────────────────

def analyze_fixture_v2(fixture: dict) -> list[dict]:
    """
    Drop-in replacement for analyzer.analyze_fixture().
    Returns the same list-of-bet-dicts format with extra metadata fields.
    """
    home_id   = fixture["home_id"]
    away_id   = fixture["away_id"]
    league_id = fixture["league_id"]
    fix_id    = fixture["fixture_id"]

    # ── Gather data
    home_form = get_recent_form(home_id, league_id)
    away_form = get_recent_form(away_id, league_id)
    h2h       = get_h2h(home_id, away_id)
    lu        = get_lineups_and_injuries(fix_id, home_id, away_id)
    odds      = get_odds(fixture)

    if not odds:
        logger.debug(f"[V2] No odds for {fixture['home_team']} vs {fixture['away_team']}")
        return []

    # ── Build xG and probabilities
    home_xg, away_xg = _build_xg(home_form, away_form, h2h, lu)
    home_p, draw_p, away_p = match_probabilities_v2(home_xg, away_xg, use_dc=True)

    logger.info(
        f"[V2] {fixture['home_team']} vs {fixture['away_team']} | "
        f"xG {home_xg:.2f}–{away_xg:.2f} | "
        f"probs {home_p:.2%}/{draw_p:.2%}/{away_p:.2%} | "
        f"form {home_form['form_str']}/{away_form['form_str']} | "
        f"inj {lu['home_injuries']}/{lu['away_injuries']}"
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
                # ── core fields (same as v1)
                "match_id":      str(fix_id),
                "home_team":     fixture["home_team"],
                "away_team":     fixture["away_team"],
                "league":        fixture["league_name"],
                "market":        market,
                "selection":     selection,
                "odds":          round(bookie_odds, 2),
                "model_prob":    round(model_prob, 4),
                "edge":          round(edge, 4),
                "kickoff":       fixture["kickoff"],
                # ── v2 metadata
                "model_version": "v2",
                "home_xg":       home_xg,
                "away_xg":       away_xg,
                "home_form":     home_form["form_str"],
                "away_form":     away_form["form_str"],
                "h2h_sample":    h2h["sample"],
                "home_injuries": lu["home_injuries"],
                "away_injuries": lu["away_injuries"],
                "dc_applied":    True,
            })

    return value_bets
