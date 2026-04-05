from __future__ import annotations
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "expenses.db"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def initialize_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL COLLATE NOCASE UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sessions_user_id
            ON sessions(user_id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gmail_tokens (
                user_id INTEGER PRIMARY KEY,
                token_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                email_id TEXT NOT NULL,
                amount REAL NOT NULL,
                type TEXT NOT NULL,
                merchant TEXT,
                upi_ref TEXT,
                date TEXT NOT NULL,
                account TEXT,
                category TEXT DEFAULT 'other',
                payment_mode TEXT DEFAULT 'debit_card',
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )

        cols = conn.execute("PRAGMA table_info(transactions)").fetchall()
        col_names = {row[1] for row in cols}
        if "user_id" not in col_names:
            conn.execute("ALTER TABLE transactions ADD COLUMN user_id INTEGER")
        if "payment_mode" not in col_names:
            conn.execute("ALTER TABLE transactions ADD COLUMN payment_mode TEXT DEFAULT 'debit_card'")
        conn.execute(
            """
            UPDATE transactions
            SET payment_mode = CASE
                WHEN upi_ref IS NOT NULL AND TRIM(upi_ref) != '' THEN 'upi'
                ELSE 'debit_card'
            END
            WHERE payment_mode IS NULL OR TRIM(payment_mode) = ''
            """
        )

        user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if user_count == 1:
            owner_id = conn.execute("SELECT id FROM users ORDER BY id LIMIT 1").fetchone()[0]
            conn.execute(
                "UPDATE transactions SET user_id = ? WHERE user_id IS NULL",
                (owner_id,),
            )

        conn.execute("DROP INDEX IF EXISTS idx_transactions_email_id")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_user_email_id
            ON transactions(user_id, email_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_user_id
            ON transactions(user_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_user_id_id
            ON transactions(user_id, id DESC)
            """
        )
        conn.commit()


if __name__ == "__main__":
    initialize_db()
