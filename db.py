"""
db.py — thin abstraction over SQLite (dev) and PostgreSQL (Railway).

Usage:
    from db import connection, P

    with connection() as conn:
        c = conn.cursor()
        c.execute(f"SELECT * FROM bets WHERE id={P}", (bet_id,))
        row = c.fetchone()          # dict-like on both backends
        conn.commit()               # no-op for SELECT; required for writes

P      = "?" (SQLite) or "%s" (PostgreSQL)
USE_PG = True when DATABASE_URL env var is present
"""

import os
import sqlite3
from contextlib import contextmanager

DATABASE_URL = os.getenv("DATABASE_URL", "")
USE_PG = bool(DATABASE_URL)

if USE_PG:
    import psycopg2
    import psycopg2.extras

# Placeholder token for parameterised queries
P = "%s" if USE_PG else "?"


@contextmanager
def connection():
    """
    Yield an open connection.  Automatically closes on exit.
    The caller is responsible for commit() / rollback().
    """
    if USE_PG:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            yield conn
        finally:
            conn.close()
    else:
        from config import DB_PATH
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = _sqlite_dict_row
        try:
            yield conn
        finally:
            conn.close()


def _sqlite_dict_row(cursor, row):
    """sqlite3 row_factory that returns dict-like objects (same as Row)."""
    return sqlite3.Row(cursor, row)


def fetchall(cursor) -> list[dict]:
    """Return rows as plain dicts regardless of backend."""
    rows = cursor.fetchall()
    if not rows:
        return []
    if USE_PG:
        return [dict(r) for r in rows]
    return [dict(r) for r in rows]


def fetchone(cursor):
    """Return a single row as a plain dict, or None."""
    row = cursor.fetchone()
    if row is None:
        return None
    if USE_PG:
        return dict(row)
    return dict(row)
