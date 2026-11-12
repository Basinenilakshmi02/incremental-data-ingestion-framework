
import sys
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from database import create_tables
from config import DATABASE_PATH


def test_database_creation():
    create_tables()

    assert DATABASE_PATH.exists()


def test_required_tables_exist():
    create_tables()

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
    """)

    tables = {row[0] for row in cursor.fetchall()}
    connection.close()

    expected_tables = {
        "customers",
        "watermarks",
        "event_history",
        "pipeline_runs",
        "rejected_records"
    }

    assert expected_tables.issubset(tables)


def test_customer_table_columns():
    create_tables()

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("PRAGMA table_info(customers)")

    columns = {row[1] for row in cursor.fetchall()}
    connection.close()

    expected_columns = {
        "customer_id",
        "customer_name",
        "email",
        "city",
        "updated_at",
        "is_deleted",
        "source_file",
        "run_id",
        "last_event_id"
    }

    assert expected_columns.issubset(columns)


def test_database_initialization_is_repeatable():
    create_tables()
    create_tables()

    assert DATABASE_PATH.exists()
