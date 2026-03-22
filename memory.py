import sqlite3
from datetime import datetime
from config import DB_PATH, BANKROLL


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS bets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT,
            home_team TEXT,
            away_team TEXT,
            league TEXT,
            market TEXT,
            selection TEXT,
            odds REAL,
            stake REAL,
            edge REAL,
            model_prob REAL,
            status TEXT DEFAULT 'pending',
            kickoff TEXT,
            created_at TEXT,
            result TEXT,
            profit REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS bankroll (
            id INTEGER PRIMARY KEY,
            amount REAL,
            initial_amount REAL,
            updated_at TEXT
        )
    """)
    c.execute(
        "INSERT OR IGNORE INTO bankroll (id, amount, initial_amount, updated_at) VALUES (1, ?, ?, ?)",
        (BANKROLL, BANKROLL, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def get_bankroll() -> tuple[float, float]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT amount, initial_amount FROM bankroll WHERE id=1")
    row = c.fetchone()
    conn.close()
    if row:
        return row[0], row[1]
    return BANKROLL, BANKROLL


def update_bankroll(amount: float):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "UPDATE bankroll SET amount=?, updated_at=? WHERE id=1",
        (amount, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def save_bet(bet: dict) -> int:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """INSERT INTO bets
           (match_id, home_team, away_team, league, market, selection,
            odds, stake, edge, model_prob, kickoff, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            bet["match_id"], bet["home_team"], bet["away_team"],
            bet.get("league", ""), bet["market"], bet["selection"],
            bet["odds"], bet["stake"], bet["edge"], bet["model_prob"],
            bet["kickoff"], datetime.now().isoformat(),
        ),
    )
    bet_id = c.lastrowid
    conn.commit()
    conn.close()
    return bet_id


def get_pending_bets() -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM bets WHERE status='pending' ORDER BY kickoff")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def settle_bet(bet_id: int, result: str) -> float | None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT odds, stake FROM bets WHERE id=?", (bet_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    odds, stake = row
    if result == "win":
        profit = round(stake * (odds - 1), 2)
    elif result == "loss":
        profit = -stake
    else:
        profit = 0.0
    c.execute(
        "UPDATE bets SET status=?, result=?, profit=? WHERE id=?",
        (result, result, profit, bet_id),
    )
    conn.commit()
    conn.close()
    current, initial = get_bankroll()
    update_bankroll(current + profit)
    return profit


def get_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result='win'  THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
            SUM(COALESCE(profit, 0)) as total_profit,
            SUM(CASE WHEN status != 'pending' THEN stake ELSE 0 END) as total_staked
        FROM bets
    """)
    row = c.fetchone()
    conn.close()
    total, wins, losses, pending, total_profit, total_staked = (v or 0 for v in row)
    settled = wins + losses
    win_rate = wins / settled if settled > 0 else 0.0
    roi = total_profit / total_staked * 100 if total_staked else 0.0
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "total_profit": total_profit,
        "total_staked": total_staked,
        "win_rate": win_rate,
        "roi": roi,
    }
