"""
Auto-settler: fetches match results from API-Football and settles pending bets.

Logic:
  - Only processes bets whose kickoff was >= 105 minutes ago (90 min + 15 min buffer)
  - Calls /fixtures?id=<fixture_id> to get full-time score
  - Skips fixtures not yet finished (status != FT / PEN / AET)
  - Maps 1X2 selection to H/D/A result code → win | loss | void
  - Sends a Telegram summary after each settle run
"""

import logging
import requests
from datetime import datetime, timezone

from agents.collector import AF_HEADERS
from config import API_FOOTBALL_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from memory import get_pending_bets, settle_bet

logger = logging.getLogger(__name__)

# Statuses Railway considers a finished match
FINISHED_STATUSES = {"FT", "AET", "PEN"}

# Minutes after kickoff before we try to settle
SETTLE_DELAY_MIN = 105


def _fetch_result(fixture_id: int) -> dict | None:
    """
    Return dict with keys: status, home_goals, away_goals
    or None if request failed.
    """
    try:
        resp = requests.get(
            f"{API_FOOTBALL_URL}/fixtures",
            headers=AF_HEADERS,
            params={"id": fixture_id, "timezone": "UTC"},
            timeout=10,
        )
        items = resp.json().get("response", [])
        if not items:
            return None
        fix = items[0]
        status     = fix["fixture"]["status"]["short"]
        score      = fix["score"]["fullTime"]
        home_goals = score.get("home")
        away_goals = score.get("away")
        return {"status": status, "home_goals": home_goals, "away_goals": away_goals}
    except Exception as e:
        logger.error(f"[Settler] API error fixture={fixture_id}: {e}")
        return None


def _resolve(selection: str, home_goals: int, away_goals: int) -> str:
    """Map bet selection + score → 'win' | 'loss' | 'void'."""
    if home_goals is None or away_goals is None:
        return "void"
    if home_goals > away_goals:
        actual = "Home Win"
    elif home_goals == away_goals:
        actual = "Draw"
    else:
        actual = "Away Win"
    return "win" if selection == actual else "loss"


def run_settler() -> list[dict]:
    """
    Check all pending bets; settle those whose match is finished.
    Returns list of settled records: {id, home_team, away_team, selection, result, profit}
    """
    now_utc = datetime.now(timezone.utc)
    pending = get_pending_bets()
    settled = []

    for bet in pending:
        # Parse kickoff
        try:
            kickoff = datetime.fromisoformat(
                bet["kickoff"].replace("Z", "+00:00")
            )
            if kickoff.tzinfo is None:
                kickoff = kickoff.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        elapsed_min = (now_utc - kickoff).total_seconds() / 60
        if elapsed_min < SETTLE_DELAY_MIN:
            continue  # too early

        fixture_id = int(bet["match_id"])
        data = _fetch_result(fixture_id)

        if data is None:
            continue

        if data["status"] not in FINISHED_STATUSES:
            logger.info(
                f"[Settler] Fixture {fixture_id} status={data['status']} — not finished yet"
            )
            continue

        result = _resolve(bet["selection"], data["home_goals"], data["away_goals"])
        profit = settle_bet(bet["id"], result)

        logger.info(
            f"[Settler] Settled bet #{bet['id']} "
            f"{bet['home_team']} vs {bet['away_team']} | "
            f"{bet['selection']} → {result} | profit={profit:+.2f}"
        )
        settled.append({
            "id":        bet["id"],
            "home_team": bet["home_team"],
            "away_team": bet["away_team"],
            "selection": bet["selection"],
            "odds":      bet["odds"],
            "stake":     bet["stake"],
            "result":    result,
            "profit":    profit,
        })

    return settled


def format_settle_report(settled: list[dict]) -> str:
    if not settled:
        return None
    total_profit = sum(s["profit"] for s in settled)
    icon_map = {"win": "✅", "loss": "❌", "void": "↩️"}
    lines = [f"<b>🏁 Авто-расчёт: {len(settled)} ставок</b>\n"]
    for s in settled:
        icon = icon_map.get(s["result"], "❓")
        lines.append(
            f"{icon} [#{s['id']}] {s['home_team']} vs {s['away_team']}\n"
            f"   {s['selection']} @ {s['odds']:.2f} | {s['stake']:.2f}€ → "
            f"<b>{s['profit']:+.2f}€</b>"
        )
    profit_icon = "📈" if total_profit >= 0 else "📉"
    lines.append(f"\n{profit_icon} Итого: <b>{total_profit:+.2f}€</b>")
    return "\n".join(lines)


async def notify_settled(settled: list[dict]):
    """Send settlement report to Telegram."""
    text = format_settle_report(settled)
    if not text:
        return
    from telegram import Bot
    try:
        bot = Bot(token=TELEGRAM_TOKEN)
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[Settler] Telegram notify error: {e}")
