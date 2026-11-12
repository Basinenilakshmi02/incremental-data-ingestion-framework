

import sqlite3

from config import DATABASE_PATH


def get_connection():
    connection = sqlite3.connect(DATABASE_PATH)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def create_tables():

    connection = get_connection()
    cursor = connection.cursor()

    # Target customer table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            email TEXT,
            city TEXT,
            updated_at TEXT NOT NULL,
            is_deleted INTEGER DEFAULT 0,
            source_file TEXT,
            run_id TEXT
        )
    """)

    # Add deterministic event tracking to existing target table
    columns = {
        row[1]
        for row in cursor.execute(
            "PRAGMA table_info(customers)"
        ).fetchall()
    }

    if "last_event_id" not in columns:
        cursor.execute("""
            ALTER TABLE customers
            ADD COLUMN last_event_id TEXT
        """)

    # Watermark table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS watermarks (
            source_name TEXT PRIMARY KEY,
            last_watermark TEXT
        )
    """)

    # Event history: one row per successfully processed event
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS event_history (
            event_id TEXT PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            operation TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            source_file TEXT NOT NULL,
            run_id TEXT NOT NULL,
            processed_at TEXT NOT NULL
        )
    """)

    # Pipeline audit table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT NOT NULL,
            input_count INTEGER DEFAULT 0,
            output_count INTEGER DEFAULT 0,
            rejected_count INTEGER DEFAULT 0,
            error_message TEXT
        )
    """)

    # Rejected records
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rejected_records (
            reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            source_file TEXT,
            customer_id INTEGER,
            raw_record TEXT NOT NULL,
            rejection_reason TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_customers_updated_at
        ON customers(updated_at)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_history_customer
        ON event_history(customer_id, updated_at)
    """)

    connection.commit()
    connection.close()


if __name__ == "__main__":
    create_tables()
    print("Database schema updated successfully!")
