

import sys
import csv
import sqlite3
from pathlib import Path

# Add the project root directory to Python's path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import database as database_module
import pipeline as pipeline_module


# Create a CSV file for testing
def create_csv(file_path, records):
    fieldnames = [
        "event_id",
        "customer_id",
        "customer_name",
        "email",
        "city",
        "updated_at",
        "operation"
    ]

    with open(file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)


# Connect to the test database
def get_connection(database_path):
    return sqlite3.connect(database_path)


# Create an isolated database for each test
# Important: Do not start this helper function with test_
def create_test_database(tmp_path, monkeypatch):
    test_database = tmp_path / "test_ingestion.db"

    monkeypatch.setattr(
        database_module, "DATABASE_PATH", test_database
    )

    monkeypatch.setattr(
        pipeline_module, "DATABASE_PATH", test_database
    )

    database_module.create_tables()

    assert test_database.exists()

    return test_database


# Test 1: Verify that new records are inserted
def test_new_records_are_inserted(tmp_path, monkeypatch):

    database_path = create_test_database(tmp_path, monkeypatch)

    records = [
        {
            "event_id": "INT001",
            "customer_id": "201",
            "customer_name": "Ravi",
            "email": "ravi@example.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-01T10:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "INT002",
            "customer_id": "202",
            "customer_name": "Priya",
            "email": "priya@example.com",
            "city": "Bangalore",
            "updated_at": "2026-11-02T10:00:00",
            "operation": "UPSERT"
        }
    ]

    source_file = tmp_path / "new_records.csv"
    create_csv(source_file, records)

    pipeline_module.ingest_file(source_file)

    connection = get_connection(database_path)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM customers")
    customer_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM event_history")
    event_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT status, output_count
        FROM pipeline_runs
        ORDER BY rowid DESC
        LIMIT 1
    """)

    run_result = cursor.fetchone()
    connection.close()

    assert customer_count == 2
    assert event_count == 2
    assert run_result == ("SUCCESS", 2)


# Test 2: Verify that rerunning the same file creates no duplicates
def test_same_file_rerun_creates_no_duplicates(tmp_path, monkeypatch):

    database_path = create_test_database(tmp_path, monkeypatch)

    records = [
        {
            "event_id": "REPLAY001",
            "customer_id": "301",
            "customer_name": "Anu",
            "email": "anu@example.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-03T10:00:00",
            "operation": "UPSERT"
        }
    ]

    source_file = tmp_path / "replay_records.csv"
    create_csv(source_file, records)

    pipeline_module.ingest_file(source_file)
    pipeline_module.ingest_file(source_file)

    connection = get_connection(database_path)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM event_history")
    event_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT output_count
        FROM pipeline_runs
        ORDER BY rowid DESC
        LIMIT 1
    """)

    second_run_output = cursor.fetchone()[0]
    connection.close()

    assert event_count == 1
    assert second_run_output == 0


# Test 3: Verify late-arriving events and delete events
def test_late_event_and_delete_event(tmp_path, monkeypatch):

    database_path = create_test_database(tmp_path, monkeypatch)

    records = [
        {
            "event_id": "LATE001",
            "customer_id": "401",
            "customer_name": "Latest Name",
            "email": "latest@example.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-05T10:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "LATE002",
            "customer_id": "401",
            "customer_name": "Older Name",
            "email": "older@example.com",
            "city": "Anantapur",
            "updated_at": "2026-11-03T10:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "DELETE001",
            "customer_id": "402",
            "customer_name": "Deleted Customer",
            "email": "deleted@example.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-06T10:00:00",
            "operation": "DELETE"
        }
    ]

    source_file = tmp_path / "late_delete_records.csv"
    create_csv(source_file, records)

    pipeline_module.ingest_file(source_file)

    connection = get_connection(database_path)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT customer_name, updated_at, is_deleted
        FROM customers
        WHERE customer_id = 401
    """)

    latest_customer = cursor.fetchone()

    cursor.execute("""
        SELECT is_deleted
        FROM customers
        WHERE customer_id = 402
    """)

    deleted_customer = cursor.fetchone()

    cursor.execute("SELECT COUNT(*) FROM event_history")
    event_count = cursor.fetchone()[0]

    connection.close()

    assert latest_customer == (
        "Latest Name", "2026-11-05T10:00:00", 0
    )

    assert deleted_customer == (1,)
    assert event_count == 3


# Test 4: Verify event ID reuse with a different payload
def test_reused_event_id_with_different_payload(tmp_path, monkeypatch):

    database_path = create_test_database(tmp_path, monkeypatch)

    original_record = {
        "event_id": "CONFLICT001",
        "customer_id": "501",
        "customer_name": "Original Name",
        "email": "original@example.com",
        "city": "Hyderabad",
        "updated_at": "2026-11-07T10:00:00",
        "operation": "UPSERT"
    }

    source_file = tmp_path / "conflict_records.csv"
    create_csv(source_file, [original_record])

    pipeline_module.ingest_file(source_file)

    changed_record = original_record.copy()
    changed_record["customer_name"] = "Changed Name"

    create_csv(source_file, [changed_record])

    pipeline_module.ingest_file(source_file)

    connection = get_connection(database_path)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT customer_name
        FROM customers
        WHERE customer_id = 501
    """)

    customer_name = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*)
        FROM rejected_records
        WHERE rejection_reason =
        'Event ID reused with different payload'
    """)

    rejected_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM event_history")
    event_count = cursor.fetchone()[0]

    connection.close()

    assert customer_name == "Original Name"
    assert rejected_count == 1
    assert event_count == 1
