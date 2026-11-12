
import json
import sqlite3

from config import DATABASE_PATH, QUARANTINE_DIR


def store_rejected_records(
    rejected_records,
    run_id,
    source_file
):
    """
    Store invalid records in the quarantine database table.

    Each rejected record retains its original source data
    and rejection reason.
    """

    connection = sqlite3.connect(DATABASE_PATH)

    try:

        cursor = connection.cursor()

        # Prevent duplicate quarantine entries
        # when the same run is retried.
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
            idx_rejected_records_unique
            ON rejected_records (
                run_id,
                source_file,
                raw_record,
                rejection_reason
            )
        """)

        inserted_count = 0

        for rejected in rejected_records:

            record = rejected["record"]
            reason = rejected["reason"]

            raw_record = json.dumps(
                record,
                sort_keys=True
            )

            cursor.execute("""
                INSERT OR IGNORE INTO rejected_records (
                    run_id,
                    source_file,
                    customer_id,
                    raw_record,
                    rejection_reason
                )
                VALUES (?, ?, ?, ?, ?)
            """, (
                run_id,
                source_file,
                record.get("customer_id"),
                raw_record,
                reason
            ))

            inserted_count += cursor.rowcount

        connection.commit()

        print(
            f"Quarantine completed: {inserted_count} "
            "new rejected records stored."
        )

        return inserted_count

    except Exception:

        connection.rollback()
        raise

    finally:

        connection.close()


def export_quarantine_records():

    connection = sqlite3.connect(DATABASE_PATH)

    try:

        cursor = connection.cursor()

        cursor.execute("""
            SELECT
                run_id,
                source_file,
                customer_id,
                raw_record,
                rejection_reason
            FROM rejected_records
            ORDER BY reject_id
        """)

        rows = cursor.fetchall()

        output_file = QUARANTINE_DIR / "rejected_records.json"

        columns = [
            "run_id",
            "source_file",
            "customer_id",
            "raw_record",
            "rejection_reason"
        ]

        records = [
            dict(zip(columns, row))
            for row in rows
        ]

        with open(output_file, "w", encoding="utf-8") as file:

            json.dump(
                records,
                file,
                indent=4
            )

        print(f"Quarantine export created: {output_file}")

    finally:

        connection.close()
