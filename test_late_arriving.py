
import csv
import sqlite3

from config import DATABASE_PATH, INPUT_DIR
from pipeline import ingest_file


TEST_FILE = INPUT_DIR / "step9_test_events.csv"


def get_customer(customer_id):
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT customer_id, customer_name, email, city,
               updated_at, is_deleted
        FROM customers
        WHERE customer_id = ?
    """, (customer_id,))

    result = cursor.fetchone()
    connection.close()

    return result


def create_test_file():
    events = [
        {
            "event_id": "STEP9_LATE_001",
            "customer_id": "101",
            "customer_name": "Late Update",
            "email": "late@example.com",
            "city": "Anantapur",
            "updated_at": "2026-11-01T12:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "STEP9_DELETE_001",
            "customer_id": "108",
            "customer_name": "Deleted Customer",
            "email": "deleted@example.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-05T12:00:00",
            "operation": "DELETE"
        }
    ]

    with open(TEST_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=events[0].keys()
        )
        writer.writeheader()
        writer.writerows(events)

    print("Test CSV file created successfully!")


def test_late_arriving_and_delete():
    # Customer 101 must already exist from the earlier sample run.
    customer_before = get_customer(101)

    assert customer_before is not None, (
        "Customer 101 does not exist. Process sample_events.csv first."
    )

    original_timestamp = customer_before[4]

    create_test_file()

    print("\nProcessing late-arriving and delete events...")

    ingest_file(TEST_FILE)

    # Verify late-arriving event was recorded.
    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM event_history
        WHERE event_id = 'STEP9_LATE_001'
    """)

    late_event_count = cursor.fetchone()[0]

    # Verify delete event was recorded.
    cursor.execute("""
        SELECT COUNT(*)
        FROM event_history
        WHERE event_id = 'STEP9_DELETE_001'
    """)

    delete_event_count = cursor.fetchone()[0]

    connection.close()

    # Customer 101 should retain its newer state.
    customer_after = get_customer(101)

    assert customer_after[4] == original_timestamp, (
        "Late-arriving event incorrectly replaced newer customer data!"
    )

    # Customer 108 should have a tombstone.
    deleted_customer = get_customer(108)

    assert deleted_customer is not None
    assert deleted_customer[5] == 1, (
        "Customer 108 was not marked as deleted!"
    )

    assert late_event_count == 1
    assert delete_event_count == 1

    print("\nSTEP 9 TEST PASSED!")
    print("Late-arriving event was recorded but did not overwrite newer data.")
    print("Delete event was recorded successfully.")
    print("Customer 108 was marked as deleted.")


if __name__ == "__main__":
    test_late_arriving_and_delete()
