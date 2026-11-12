
import sqlite3

from config import DATABASE_PATH


def check_output():

    connection = sqlite3.connect(DATABASE_PATH)

    cursor = connection.cursor()

    print("\nCUSTOMER TABLE")
    print("=" * 60)

    cursor.execute("""
        SELECT
            customer_id,
            customer_name,
            email,
            city,
            updated_at,
            is_deleted
        FROM customers
        ORDER BY customer_id
    """)

    for row in cursor.fetchall():
        print(row)

    print("\nPIPELINE RUN HISTORY")
    print("=" * 60)

    cursor.execute("""
        SELECT
            run_id,
            status,
            input_count,
            output_count,
            rejected_count
        FROM pipeline_runs
        ORDER BY start_time DESC
        LIMIT 5
    """)

    for row in cursor.fetchall():
        print(row)

    print("\nWATERMARK")
    print("=" * 60)

    cursor.execute("""
        SELECT source_name, last_watermark
        FROM watermarks
    """)

    for row in cursor.fetchall():
        print(row)

    print("\nEVENT HISTORY COUNT")

    cursor.execute("""
        SELECT COUNT(*)
        FROM event_history
    """)

    print(cursor.fetchone()[0])

    connection.close()


if __name__ == "__main__":
    check_output()
