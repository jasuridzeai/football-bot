import logging
import threading
from telegram import Update, BotCommand
from telegram.ext import Application, CommandHandler, ContextTypes

from config import TELEGRAM_TOKEN, WINDOW_HOURS
from memory import init_db, get_bankroll, get_pending_bets, settle_bet, get_stats
from scheduler import start_scheduler, generate_coupon, format_coupon, get_schedule_status
from agents.settler import run_settler, format_settle_report
from agents.analyzer_v2 import analyze_fixture_v2
from agents.collector import get_upcoming_fixtures
from agents.bankroll import add_stakes, is_stop_loss_triggered
from agents.validator import validate_and_select
from agents.lineup import form_emoji
from backtest import run_backtest, format_backtest_report
from memory import save_bet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── V2 coupon pipeline ─────────────────────────────────────────────────────────

def generate_coupon_v2() -> list[dict]:
    """Full pipeline using analyzer_v2 (form + H2H + DC + injuries)."""
    if is_stop_loss_triggered():
        return []

    fixtures = get_upcoming_fixtures(hours_from_now=0, window=WINDOW_HOURS)
    all_bets: list[dict] = []
    for fix in fixtures:
        all_bets.extend(analyze_fixture_v2(fix))

    selected = validate_and_select(all_bets)
    staked   = add_stakes(selected)

    saved = []
    for bet in staked:
        bid = save_bet(bet)
        saved.append({**bet, "id": bid})
    return saved


def format_coupon_v2(bets: list[dict]) -> str:
    if not bets:
        return "Нет value bets (v2) или активен стоп-лосс."
    lines = ["<b>⚽ Купон v2 (form + H2H + Dixon-Coles):</b>\n"]
    for i, bet in enumerate(bets, 1):
        home_f = form_emoji(bet.get("home_form", "?????"))
        away_f = form_emoji(bet.get("away_form", "?????"))
        h2h    = bet.get("h2h_sample", 0)
        inj_h  = bet.get("home_injuries", 0)
        inj_a  = bet.get("away_injuries", 0)
        xg_h   = bet.get("home_xg", 0)
        xg_a   = bet.get("away_xg", 0)

        lines.append(
            f"{i}. <b>{bet['home_team']} vs {bet['away_team']}</b> [{bet.get('league','')}]\n"
            f"   📌 {bet['selection']} @ <b>{bet['odds']:.2f}</b>\n"
            f"   📊 Edge: <b>{bet['edge']*100:.1f}%</b> | Ставка: <b>{bet['stake']:.2f}€</b>\n"
            f"   ⚡ xG {xg_h:.2f}–{xg_a:.2f} | H2H: {h2h} матчей\n"
            f"   {home_f} vs {away_f}"
            + (f" | 🤕 {inj_h}/{inj_a}" if inj_h or inj_a else "") + "\n"
            f"   🕐 {bet['kickoff'][:16]} UTC\n"
        )
    return "\n".join(lines)


# ── Handlers ───────────────────────────────────────────────────────────────────

async def cmd_coupon(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Генерирую купон (v1)…")
    bets = generate_coupon()
    await update.message.reply_text(
        format_coupon(bets) if bets else "Нет value bets или активен стоп-лосс.",
        parse_mode="HTML",
    )


async def cmd_coupon_v2(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "⏳ Генерирую купон v2 (форма + H2H + Dixon-Coles + травмы)…\n"
        "Это займёт ~30 сек — запрашиваю свежие данные."
    )

    bot  = update.get_bot()
    chat = update.effective_chat.id

    def _run():
        bets   = generate_coupon_v2()
        report = format_coupon_v2(bets)
        import asyncio
        asyncio.run(bot.send_message(chat_id=chat, text=report, parse_mode="HTML"))

    threading.Thread(target=_run, daemon=True).start()


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    s = get_stats()
    current, initial = get_bankroll()
    profit_pct = (current - initial) / initial * 100 if initial else 0
    text = (
        "<b>📊 Статистика:</b>\n\n"
        f"Всего ставок: {s['total']}\n"
        f"✅ Выиграно:  {s['wins']}\n"
        f"❌ Проиграно: {s['losses']}\n"
        f"⏳ Ожидают:   {s['pending']}\n"
        f"Win rate: {s['win_rate']*100:.1f}%\n"
        f"Прибыль:  {s['total_profit']:+.2f}€\n"
        f"ROI:      {s['roi']:+.1f}%\n"
        f"Банкролл: {current:.2f}€ ({profit_pct:+.1f}%)"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    current, initial = get_bankroll()
    drawdown = max((initial - current) / initial * 100, 0) if initial else 0
    await update.message.reply_text(
        f"<b>💰 Банкролл:</b> {current:.2f}€\n"
        f"Начальный: {initial:.2f}€\n"
        f"Просадка:  {drawdown:.1f}%",
        parse_mode="HTML",
    )


async def cmd_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    bets = get_pending_bets()
    if not bets:
        await update.message.reply_text("Нет ожидающих ставок.")
        return
    lines = ["<b>⏳ Ожидающие ставки:</b>\n"]
    for bet in bets:
        lines.append(
            f"[#{bet['id']}] {bet['home_team']} vs {bet['away_team']}\n"
            f"  {bet['selection']} @ {bet['odds']:.2f} | "
            f"{bet['stake']:.2f}€ | {bet['kickoff'][:16]}"
        )
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def cmd_settle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) != 2:
        await update.message.reply_text(
            "Использование: /settle <bet_id> <win|loss|void>"
        )
        return
    try:
        bet_id = int(args[0])
        result = args[1].lower()
        if result not in ("win", "loss", "void"):
            await update.message.reply_text("Результат: win | loss | void")
            return
        profit = settle_bet(bet_id, result)
        if profit is None:
            await update.message.reply_text(f"Ставка #{bet_id} не найдена.")
            return
        icon = "✅" if result == "win" else ("❌" if result == "loss" else "↩️")
        await update.message.reply_text(
            f"{icon} Ставка #{bet_id} → <b>{result}</b>\nПрибыль: {profit:+.2f}€",
            parse_mode="HTML",
        )
    except ValueError:
        await update.message.reply_text("bet_id должен быть числом.")


async def cmd_settle_auto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ Проверяю результаты матчей…")

    def _run():
        settled = run_settler()
        report  = format_settle_report(settled) or "Нет ставок готовых к расчёту."
        import asyncio
        asyncio.run(update.get_bot().send_message(
            chat_id=update.effective_chat.id,
            text=report, parse_mode="HTML",
        ))

    threading.Thread(target=_run, daemon=True).start()


async def cmd_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        f"<b>🗓 Планировщик:</b>\n{get_schedule_status()}",
        parse_mode="HTML",
    )


async def cmd_backtest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args   = ctx.args
    season = int(args[0]) if args and args[0].isdigit() else None
    season_str = str(season) if season else "текущий сезон"

    await update.message.reply_text(
        f"⏳ Запускаю бэктест ({season_str})…\nЭто займёт 1-3 минуты."
    )
    bot  = update.get_bot()
    chat = update.effective_chat.id

    def _run():
        try:
            result = run_backtest(season=season)
            report = format_backtest_report(result)
        except Exception as e:
            report = f"❌ Ошибка бэктеста: {e}"

        import asyncio
        async def _send():
            for i in range(0, len(report), 4000):
                await bot.send_message(
                    chat_id=chat, text=report[i:i+4000], parse_mode="HTML"
                )
        asyncio.run(_send())

    threading.Thread(target=_run, daemon=True).start()


# ── Init ───────────────────────────────────────────────────────────────────────

async def post_init(app: Application):
    await app.bot.set_my_commands([
        BotCommand("coupon",      "Купон v1 (сезонная статистика)"),
        BotCommand("coupon_v2",   "Купон v2 (форма + H2H + Dixon-Coles + травмы)"),
        BotCommand("stats",       "Статистика ставок"),
        BotCommand("balance",     "Текущий банкролл"),
        BotCommand("pending",     "Ожидающие ставки"),
        BotCommand("settle",      "Закрыть ставку вручную: /settle <id> <win|loss|void>"),
        BotCommand("settle_auto", "Авто-расчёт всех завершённых ставок"),
        BotCommand("schedule",    "Статус планировщика"),
        BotCommand("backtest",    "Бэктест модели: /backtest [год]"),
    ])


def main():
    init_db()
    start_scheduler()

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("coupon",      cmd_coupon))
    app.add_handler(CommandHandler("coupon_v2",   cmd_coupon_v2))
    app.add_handler(CommandHandler("stats",       cmd_stats))
    app.add_handler(CommandHandler("balance",     cmd_balance))
    app.add_handler(CommandHandler("pending",     cmd_pending))
    app.add_handler(CommandHandler("settle",      cmd_settle))
    app.add_handler(CommandHandler("settle_auto", cmd_settle_auto))
    app.add_handler(CommandHandler("schedule",    cmd_schedule))
    app.add_handler(CommandHandler("backtest",    cmd_backtest))

    logger.info("⚽ Football Bot запущен")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
