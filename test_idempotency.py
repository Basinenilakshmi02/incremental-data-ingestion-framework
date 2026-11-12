import sqlite3

from config import DATABASE_PATH, INPUT_DIR
from database import create_tables
from pipeline import ingest_file


def get_database_snapshot():
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT customer_id, customer_name, email, city,
               updated_at, is_deleted
        FROM customers
        ORDER BY customer_id
    """)
    customers = cursor.fetchall()

    cursor.execute("SELECT COUNT(*) FROM event_history")
    event_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT source_name, last_watermark
        FROM watermarks
        ORDER BY source_name
    """)
    watermarks = cursor.fetchall()

    connection.close()

    return customers, event_count, watermarks


def test_idempotency():
    create_tables()

    sample_file = INPUT_DIR / "sample_events.csv"

    print("Running the pipeline for the first time...")
    ingest_file(sample_file)

    first_snapshot = get_database_snapshot()

    print("Running the pipeline for the second time...")
    ingest_file(sample_file)

    second_snapshot = get_database_snapshot()

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT status, input_count, output_count, rejected_count
        FROM pipeline_runs
        ORDER BY start_time DESC
        LIMIT 1
    """)

    latest_run = cursor.fetchone()
    connection.close()

    assert first_snapshot == second_snapshot, (
        "Data changed during the second run!"
    )

    assert latest_run is not None
    assert latest_run[0] == "SUCCESS", "Second run was not successful!"
    assert latest_run[2] == 0, (
        "Second run unexpectedly applied customer changes!"
    )

    print("\nIDEMPOTENCY TEST PASSED!")
    print("No duplicate events were created.")
    print("Existing customer records remained unchanged.")
    print("Second run applied zero customer changes.")


if __name__ == "__main__":
    test_idempotency()