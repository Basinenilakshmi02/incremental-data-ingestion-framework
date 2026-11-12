
import sqlite3

from config import DATABASE_PATH


def run_reconciliation():

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()

    print("\nDATA RECONCILIATION REPORT")
    print("=" * 40)

    # 1. Count processed events
    cursor.execute("""
        SELECT COUNT(*)
        FROM event_history
    """)
    event_count = cursor.fetchone()[0]

    # 2. Count customer records
    cursor.execute("""
        SELECT COUNT(*)
        FROM customers
    """)
    customer_count = cursor.fetchone()[0]

    # 3. Count active customers
    cursor.execute("""
        SELECT COUNT(*)
        FROM customers
        WHERE is_deleted = 0
    """)
    active_count = cursor.fetchone()[0]

    # 4. Count deleted customers
    cursor.execute("""
        SELECT COUNT(*)
        FROM customers
        WHERE is_deleted = 1
    """)
    deleted_count = cursor.fetchone()[0]

    # 5. Find events without customer records
    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT customer_id
            FROM event_history
        ) e
        LEFT JOIN customers c
            ON e.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    missing_customers = cursor.fetchone()[0]

    # 6. Verify latest event against customer state
    cursor.execute("""
        WITH latest_events AS (
            SELECT
                customer_id,
                event_id,
                operation,
                updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY updated_at DESC, event_id DESC
                ) AS row_num
            FROM event_history
        )
        SELECT COUNT(*)
        FROM latest_events e
        JOIN customers c
            ON e.customer_id = c.customer_id
        WHERE e.row_num = 1
          AND (
              e.event_id != c.last_event_id
              OR e.updated_at != c.updated_at
              OR (
                  e.operation = 'DELETE'
                  AND c.is_deleted != 1
              )
              OR (
                  e.operation = 'UPSERT'
                  AND c.is_deleted != 0
              )
          )
    """)
    mismatched_customers = cursor.fetchone()[0]

    # 7. Verify source watermarks
    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT
                source_file,
                MAX(updated_at) AS max_timestamp
            FROM event_history
            GROUP BY source_file
        ) e
        LEFT JOIN watermarks w
            ON e.source_file = w.source_name
        WHERE w.source_name IS NULL
           OR w.last_watermark != e.max_timestamp
    """)
    watermark_mismatches = cursor.fetchone()[0]

    connection.close()

    # Display reconciliation metrics
    print("Total processed events:", event_count)
    print("Total customer records:", customer_count)
    print("Active customers:", active_count)
    print("Deleted customers:", deleted_count)
    print("Missing customer records:", missing_customers)
    print("Customer state mismatches:", mismatched_customers)
    print("Watermark mismatches:", watermark_mismatches)

    # Overall reconciliation status
    success = (
        missing_customers == 0
        and mismatched_customers == 0
        and watermark_mismatches == 0
    )

    print("-" * 40)

    if success:
        print("RECONCILIATION STATUS: PASSED")
    else:
        print("RECONCILIATION STATUS: FAILED")

    return success


if __name__ == "__main__":
    run_reconciliation()
