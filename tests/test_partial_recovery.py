
import csv
import sqlite3
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import database
import pipeline
from batch_reader import read_csv_in_batches


def test_partial_failure_recovery(tmp_path, monkeypatch):

    # Create a separate test database
    test_db = tmp_path / "recovery_test.db"

    monkeypatch.setattr(database, "DATABASE_PATH", test_db)
    monkeypatch.setattr(pipeline, "DATABASE_PATH", test_db)

    database.create_tables()

    # Create a test CSV containing 5 records
    input_file = tmp_path / "recovery_events.csv"

    records = []

    for i in range(1, 6):
        records.append({
            "event_id": f"REC{i}",
            "customer_id": str(i),
            "customer_name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "city": "Hyderabad",
            "updated_at": f"2026-11-0{i}T10:00:00",
            "operation": "UPSERT"
        })

    with open(input_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=records[0].keys()
        )
        writer.writeheader()
        writer.writerows(records)

    # First run: process two records, then simulate a failure
    original_reader = pipeline.read_csv_in_batches

    def failing_reader(file_path, batch_size=1000):

        for batch_number, batch in enumerate(
            original_reader(file_path, batch_size=2),
            start=1
        ):

            if batch_number == 2:
                raise RuntimeError("Simulated batch failure")

            yield batch

    monkeypatch.setattr(
        pipeline,
        "read_csv_in_batches",
        failing_reader
    )

    with pytest.raises(RuntimeError, match="Simulated batch failure"):
        pipeline.ingest_file(input_file)

    # Verify that the first batch was committed
    connection = sqlite3.connect(test_db)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM event_history")
    saved_events = cursor.fetchone()[0]

    cursor.execute("""
        SELECT status
        FROM pipeline_runs
        ORDER BY rowid DESC
        LIMIT 1
    """)
    failed_status = cursor.fetchone()[0]

    connection.close()

    assert saved_events == 2
    assert failed_status == "FAILED"

    # Second run: restore normal batch reader and retry
    monkeypatch.setattr(
        pipeline,
        "read_csv_in_batches",
        original_reader
    )

    pipeline.ingest_file(input_file)

    # Verify all five events are now recorded
    connection = sqlite3.connect(test_db)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM event_history")
    final_events = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM customers")
    final_customers = cursor.fetchone()[0]

    cursor.execute("""
        SELECT status
        FROM pipeline_runs
        ORDER BY rowid DESC
        LIMIT 1
    """)
    final_status = cursor.fetchone()[0]

    connection.close()

    assert final_events == 5
    assert final_customers == 5
    assert final_status == "SUCCESS"
