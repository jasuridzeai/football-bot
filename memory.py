from datetime import datetime
from config import DB_PATH, BANKROLL
from db import connection, P, USE_PG, fetchone, fetchall


def init_db():
    with connection() as conn:
        c = conn.cursor()
        if USE_PG:
            c.execute("""
                CREATE TABLE IF NOT EXISTS bets (
                    id SERIAL PRIMARY KEY,
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
            # Unique partial index: no two pending bets for same match+selection
            c.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_bet
                ON bets (match_id, selection)
                WHERE status = 'pending'
            """)
        else:
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
            # SQLite partial unique index for deduplication
            c.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_bet
                ON bets (match_id, selection)
                WHERE status = 'pending'
            """)
        if USE_PG:
            c.execute(
                f"INSERT INTO bankroll (id, amount, initial_amount, updated_at) VALUES ({P},{P},{P},{P}) ON CONFLICT (id) DO NOTHING",
                (1, BANKROLL, BANKROLL, datetime.now().isoformat()),
            )
        else:
            c.execute(
                f"INSERT OR IGNORE INTO bankroll (id, amount, initial_amount, updated_at) VALUES (1, {P}, {P}, {P})",
                (BANKROLL, BANKROLL, datetime.now().isoformat()),
            )
        conn.commit()


def get_bankroll() -> tuple[float, float]:
    with connection() as conn:
        c = conn.cursor()
        c.execute(f"SELECT amount, initial_amount FROM bankroll WHERE id={P}", (1,))
        row = fetchone(c)
    if row:
        return row["amount"], row["initial_amount"]
    return BANKROLL, BANKROLL


def update_bankroll(amount: float):
    with connection() as conn:
        c = conn.cursor()
        c.execute(
            f"UPDATE bankroll SET amount={P}, updated_at={P} WHERE id=1",
            (amount, datetime.now().isoformat()),
        )
        conn.commit()


def save_bet(bet: dict) -> int | None:
    """
    Insert a new pending bet.  Returns the new row id, or None if a pending
    bet with the same match_id + selection already exists (deduplication).
    """
    with connection() as conn:
        c = conn.cursor()
        try:
            if USE_PG:
                c.execute(
                    f"""INSERT INTO bets
                       (match_id, home_team, away_team, league, market, selection,
                        odds, stake, edge, model_prob, kickoff, created_at)
                       VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})
                       ON CONFLICT DO NOTHING
                       RETURNING id""",
                    (
                        bet["match_id"], bet["home_team"], bet["away_team"],
                        bet.get("league", ""), bet["market"], bet["selection"],
                        bet["odds"], bet["stake"], bet["edge"], bet["model_prob"],
                        bet["kickoff"], datetime.now().isoformat(),
                    ),
                )
                row = c.fetchone()
                conn.commit()
                return dict(row)["id"] if row else None
            else:
                c.execute(
                    f"""INSERT OR IGNORE INTO bets
                       (match_id, home_team, away_team, league, market, selection,
                        odds, stake, edge, model_prob, kickoff, created_at)
                       VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
                    (
                        bet["match_id"], bet["home_team"], bet["away_team"],
                        bet.get("league", ""), bet["market"], bet["selection"],
                        bet["odds"], bet["stake"], bet["edge"], bet["model_prob"],
                        bet["kickoff"], datetime.now().isoformat(),
                    ),
                )
                bet_id = c.lastrowid if c.rowcount > 0 else None
                conn.commit()
                return bet_id
        except Exception:
            conn.rollback()
            raise


def get_pending_bets() -> list[dict]:
    with connection() as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM bets WHERE status={P} ORDER BY kickoff", ("pending",))
        return fetchall(c)


def settle_bet(bet_id: int, result: str) -> float | None:
    with connection() as conn:
        c = conn.cursor()
        try:
            c.execute(f"SELECT odds, stake FROM bets WHERE id={P}", (bet_id,))
            row = fetchone(c)
            if not row:
                return None
            odds, stake = row["odds"], row["stake"]
            if result == "win":
                profit = round(stake * (odds - 1), 2)
            elif result == "loss":
                profit = -stake
            else:
                profit = 0.0
            c.execute(
                f"UPDATE bets SET status='settled', result={P}, profit={P} WHERE id={P}",
                (result, profit, bet_id),
            )
            c.execute(
                f"UPDATE bankroll SET amount=amount+{P}, updated_at={P} WHERE id=1",
                (profit, datetime.now().isoformat()),
            )
            conn.commit()
            return profit
        except Exception:
            conn.rollback()
            raise


def get_stats() -> dict:
    with connection() as conn:
        c = conn.cursor()
        c.execute(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result='win'  THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) as pending,
                SUM(COALESCE(profit, 0)) as total_profit,
                SUM(CASE WHEN status = 'settled' THEN stake ELSE 0 END) as total_staked
            FROM bets
        """)
        row = fetchone(c)
    if row is None:
        row = {}
    total        = row.get("total", 0) or 0
    wins         = row.get("wins", 0) or 0
    losses       = row.get("losses", 0) or 0
    pending      = row.get("pending", 0) or 0
    total_profit = row.get("total_profit", 0) or 0
    total_staked = row.get("total_staked", 0) or 0
    settled  = wins + losses
    win_rate = wins / settled if settled > 0 else 0.0
    roi      = total_profit / total_staked * 100 if total_staked else 0.0
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
