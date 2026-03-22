import asyncio
import threading
import time
import logging
from datetime import datetime, timedelta

from agents.collector import get_upcoming_fixtures
from agents.analyzer_v2 import analyze_fixture_v2 as analyze_fixture
from agents.bankroll import add_stakes, is_stop_loss_triggered
from agents.validator import validate_and_select
from agents.settler import run_settler, notify_settled
from memory import save_bet, init_db
from config import PRE_MATCH_HOURS, WINDOW_HOURS, DAILY_RESET_TIME, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

# fixture_id -> datetime when analysis was scheduled
_scheduled: dict[int, datetime] = {}
_bot_app = None  # set by bot.py

SETTLER_INTERVAL_MIN = 30  # run auto-settler every 30 minutes


def set_bot(app):
    global _bot_app
    _bot_app = app


# ── Coupon generation ──────────────────────────────────────────────────────────

def generate_coupon() -> list[dict]:
    """Run the full pipeline and persist bets. Returns saved bets with IDs."""
    if is_stop_loss_triggered():
        logger.warning("[Scheduler] Stop-loss active — no bets generated.")
        return []

    fixtures = get_upcoming_fixtures(hours_from_now=0, window=WINDOW_HOURS)
    all_bets: list[dict] = []
    for fix in fixtures:
        all_bets.extend(analyze_fixture(fix))

    selected = validate_and_select(all_bets)
    staked   = add_stakes(selected)

    saved = []
    for bet in staked:
        bid = save_bet(bet)
        saved.append({**bet, "id": bid})
    return saved


def format_coupon(bets: list[dict]) -> str:
    if not bets:
        return "Нет подходящих value bets."
    lines = ["<b>⚽ Купон value bets:</b>\n"]
    for i, bet in enumerate(bets, 1):
        comment = bet.get("comment", "")
        lines.append(
            f"{i}. <b>{bet['home_team']} vs {bet['away_team']}</b> [{bet.get('league','')}]\n"
            f"   📌 {bet['selection']} @ <b>{bet['odds']:.2f}</b>\n"
            f"   📊 Edge: <b>{bet['edge']*100:.1f}%</b> | Ставка: <b>{bet['stake']:.2f}€</b>\n"
            f"   🕐 {bet['kickoff'][:16]} UTC\n"
            + (f"\n   💡 {comment}\n" if comment else "")
        )
    return "\n".join(lines)


# ── Notifications ──────────────────────────────────────────────────────────────

async def _notify(text: str):
    from telegram import Bot
    bot = Bot(token=TELEGRAM_TOKEN)
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[Scheduler] Telegram notify error: {e}")


# ── Pre-match analysis ─────────────────────────────────────────────────────────

def _run_for_fixture(fixture: dict):
    """Analyse a single fixture and push notification if bets found."""
    if is_stop_loss_triggered():
        return

    bets     = analyze_fixture(fixture)
    selected = validate_and_select(bets)
    staked   = add_stakes(selected)

    if not staked:
        return

    saved = []
    for bet in staked:
        bid = save_bet(bet)
        saved.append({**bet, "id": bid})

    asyncio.run(_notify(format_coupon(saved)))


# ── Auto-settler ───────────────────────────────────────────────────────────────

def _run_settler():
    """Run settler and send Telegram report if any bets were settled."""
    try:
        settled = run_settler()
        if settled:
            asyncio.run(notify_settled(settled))
    except Exception as e:
        logger.error(f"[Scheduler] Settler error: {e}")


# ── Main loop ──────────────────────────────────────────────────────────────────

def _scheduler_loop():
    init_db()
    last_daily_reset: datetime | None = None
    last_settler_run: datetime = datetime.utcnow() - timedelta(minutes=SETTLER_INTERVAL_MIN)

    while True:
        now = datetime.now()
        now_utc = datetime.utcnow()

        # ── Daily reset
        reset_today = datetime.strptime(
            f"{now.date()} {DAILY_RESET_TIME}", "%Y-%m-%d %H:%M"
        )
        if (last_daily_reset is None or last_daily_reset.date() < now.date()) \
                and now >= reset_today:
            logger.info("[Scheduler] Daily reset.")
            last_daily_reset = now

        # ── Pre-match coupon: queue analysis 2.5 h before kickoff
        upcoming = get_upcoming_fixtures(
            hours_from_now=PRE_MATCH_HOURS - 0.25, window=0.5
        )
        for fix in upcoming:
            fid = fix["fixture_id"]
            if fid not in _scheduled:
                logger.info(
                    f"[Scheduler] Queuing: {fix['home_team']} vs {fix['away_team']} "
                    f"@ {fix['kickoff'][:16]}"
                )
                _scheduled[fid] = now_utc
                t = threading.Thread(
                    target=_run_for_fixture, args=(fix,), daemon=True
                )
                t.start()

        # ── Auto-settler: run every SETTLER_INTERVAL_MIN minutes
        if (now_utc - last_settler_run).total_seconds() >= SETTLER_INTERVAL_MIN * 60:
            logger.info("[Scheduler] Running auto-settler...")
            last_settler_run = now_utc
            threading.Thread(target=_run_settler, daemon=True).start()

        time.sleep(600)  # poll every 10 minutes


def start_scheduler():
    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()
    logger.info("[Scheduler] Background scheduler started.")


def get_schedule_status() -> str:
    lines = []
    if _scheduled:
        for fid, ts in _scheduled.items():
            lines.append(f"• Fixture #{fid} — запущен в {ts.strftime('%H:%M UTC')}")
    else:
        lines.append("Нет запланированных анализов.")
    lines.append(f"\n⚙️ Авто-расчёт каждые {SETTLER_INTERVAL_MIN} мин")
    return "\n".join(lines)
