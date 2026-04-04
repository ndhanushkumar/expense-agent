from __future__ import annotations
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "expenses.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_id TEXT NOT NULL,
                amount REAL NOT NULL,
                type TEXT NOT NULL,
                merchant TEXT,
                upi_ref TEXT,
                date TEXT NOT NULL,
                account TEXT,
                category TEXT DEFAULT 'other'
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_email_id
            ON transactions(email_id)
            """
        )
        conn.commit()


if __name__ == "__main__":
    initialize_db()