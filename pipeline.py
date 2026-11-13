
import csv
import json
import hashlib
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta

from config import (
    DATABASE_PATH,
    INPUT_DIR,
    SOURCE_NAME,
    WATERMARK_LOOKBACK_MINUTES,
    WATERMARK_COLUMN
)
from validation import validate_record
from batch_reader import read_csv_in_batches
from logger_config import get_logger

logger = get_logger()


def get_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
def get_incremental_start_time(connection, source_name):
    """
    Read the previous watermark and calculate
    the start time using the configured lookback.
    """

    cursor = connection.cursor()

    cursor.execute("""
        SELECT last_watermark
        FROM watermarks
        WHERE source_name = ?
    """, (source_name,))

    result = cursor.fetchone()

    # First run: process all available records
    if result is None or result[0] is None:
        return None

    last_watermark = datetime.strptime(
        result[0],
        "%Y-%m-%dT%H:%M:%S"
    )

    # Recheck the previous 24 hours
    incremental_start = (
        last_watermark
        - timedelta(minutes=WATERMARK_LOOKBACK_MINUTES)
    )

    return incremental_start.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def get_payload_hash(record):
    payload = json.dumps(record, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ingest_file(file_path):

    run_id = str(uuid.uuid4())
    source_file = file_path.name
    source_name = SOURCE_NAME

    start_time = get_timestamp()

    input_count = 0
    output_count = 0
    rejected_count = 0

    seen_event_ids = set()

    connection = sqlite3.connect(DATABASE_PATH)
    cursor = connection.cursor()
    incremental_start = get_incremental_start_time(
    connection,
    source_name
)
    

    logger.info(
    "Incremental start time: %s",
    incremental_start or "FIRST RUN - FULL LOAD"
)

    logger.info(
        "Batch pipeline started | Run ID: %s | Source: %s",
        run_id,
        source_file
    )

    try:

        # STEP 1: Validate CSV structure
        with open(file_path, "r", encoding="utf-8", newline="") as file:

            reader = csv.DictReader(file)

            expected_fields = {
                "event_id",
                "customer_id",
                "customer_name",
                "email",
                "city",
                "updated_at",
                "operation"
            }

            if (
                reader.fieldnames is None
                or len(reader.fieldnames) != len(set(reader.fieldnames))
                or set(reader.fieldnames) != expected_fields
            ):
                raise ValueError("Unexpected source schema")

        # STEP 2: Create pipeline run record
        cursor.execute("""
            INSERT INTO pipeline_runs (
                run_id,
                start_time,
                status,
                input_count
            )
            VALUES (?, ?, ?, ?)
        """, (
            run_id,
            start_time,
            "RUNNING",
            0
        ))

        connection.commit()

        # STEP 3: Read and process one batch at a time
        for batch_number, batch in enumerate(
            read_csv_in_batches(file_path), start=1
        ):

            input_count += len(batch)

            batch_output_count = 0
            batch_rejected_count = 0

            # Start a separate transaction for this batch
            connection.execute("BEGIN")

            try:

                for record in batch:

                    is_valid, reason = validate_record(record)

                    # Reject invalid records
                    if not is_valid:

                        cursor.execute("""
                            INSERT INTO rejected_records (
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
                            json.dumps(record, sort_keys=True),
                            reason
                        ))

                        batch_rejected_count += 1
                        continue

                    event_id = record["event_id"].strip()
                    customer_id = int(record["customer_id"])
                    operation = record["operation"].strip().upper()
                    updated_at = record["updated_at"]

                    # Detect duplicate events within this run
                    if event_id in seen_event_ids:

                        cursor.execute("""
                            INSERT INTO rejected_records (
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
                            customer_id,
                            json.dumps(record, sort_keys=True),
                            "Duplicate event ID within batch/run"
                        ))

                        batch_rejected_count += 1
                        continue

                    seen_event_ids.add(event_id)

                    payload_hash = get_payload_hash(record)

                    # Check whether event was already processed
                    cursor.execute("""
                        SELECT payload_hash
                        FROM event_history
                        WHERE event_id = ?
                    """, (event_id,))

                    existing_event = cursor.fetchone()

                    if existing_event:

                        if existing_event[0] != payload_hash:

                            cursor.execute("""
                                INSERT INTO rejected_records (
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
                                customer_id,
                                json.dumps(record, sort_keys=True),
                                "Event ID reused with different payload"
                            ))

                            batch_rejected_count += 1

                        continue
                    # Skip records outside the incremental lookback window
                    if (
                        incremental_start is not None
                        and record[WATERMARK_COLUMN] < incremental_start
                    ):
                        continue

                    # Store the event in event history
                    cursor.execute("""
                        INSERT INTO event_history (
                            event_id,
                            customer_id,
                            updated_at,
                            operation,
                            payload_hash,
                            source_file,
                            run_id,
                            processed_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        event_id,
                        customer_id,
                        updated_at,
                        operation,
                        payload_hash,
                        source_file,
                        run_id,
                        get_timestamp()
                    ))

                    # Get current customer state
                    cursor.execute("""
                        SELECT updated_at, last_event_id
                        FROM customers
                        WHERE customer_id = ?
                    """, (customer_id,))

                    current = cursor.fetchone()

                    # Check whether the incoming event is newer
                    is_newer = (
                        current is None
                        or updated_at > current[0]
                        or (
                            updated_at == current[0]
                            and event_id > (current[1] or "")
                        )
                    )

                    if not is_newer:
                        continue

                    # Apply UPSERT
                    if operation == "UPSERT":

                        cursor.execute("""
                            INSERT INTO customers (
                                customer_id,
                                customer_name,
                                email,
                                city,
                                updated_at,
                                is_deleted,
                                source_file,
                                run_id,
                                last_event_id
                            )
                            VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)

                            ON CONFLICT(customer_id)
                            DO UPDATE SET
                                customer_name = excluded.customer_name,
                                email = excluded.email,
                                city = excluded.city,
                                updated_at = excluded.updated_at,
                                is_deleted = 0,
                                source_file = excluded.source_file,
                                run_id = excluded.run_id,
                                last_event_id = excluded.last_event_id
                        """, (
                            customer_id,
                            record["customer_name"],
                            record["email"],
                            record["city"],
                            updated_at,
                            source_file,
                            run_id,
                            event_id
                        ))

                    # Apply DELETE / tombstone
                    elif operation == "DELETE":

                        cursor.execute("""
                            INSERT INTO customers (
                                customer_id,
                                customer_name,
                                email,
                                city,
                                updated_at,
                                is_deleted,
                                source_file,
                                run_id,
                                last_event_id
                            )
                            VALUES (?, '[deleted]', NULL, NULL, ?, 1, ?, ?, ?)

                            ON CONFLICT(customer_id)
                            DO UPDATE SET
                                updated_at = excluded.updated_at,
                                is_deleted = 1,
                                source_file = excluded.source_file,
                                run_id = excluded.run_id,
                                last_event_id = excluded.last_event_id
                        """, (
                            customer_id,
                            updated_at,
                            source_file,
                            run_id,
                            event_id
                        ))

                    batch_output_count += 1

                # Save this batch before moving to the next one
                connection.commit()

                output_count += batch_output_count
                rejected_count += batch_rejected_count

                logger.info(
                    "Batch %s completed | Records: %s | Applied: %s | Rejected: %s",
                    batch_number,
                    len(batch),
                    batch_output_count,
                    batch_rejected_count
                )

            except Exception:
                # Roll back only the failed batch
                connection.rollback()
                raise

        # STEP 4: Update source watermark after all batches
        cursor.execute("""
    SELECT MAX(updated_at)
    FROM event_history
""")
        max_timestamp = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO watermarks (
                source_name,
                last_watermark
            )
            VALUES (?, ?)

            ON CONFLICT(source_name)
            DO UPDATE SET
                last_watermark =
                    CASE
                        WHEN excluded.last_watermark >
                             COALESCE(watermarks.last_watermark, '')
                        THEN excluded.last_watermark
                        ELSE watermarks.last_watermark
                    END
        """, (
            source_name,
            max_timestamp
        ))

        # STEP 5: Update successful run details
        cursor.execute("""
            UPDATE pipeline_runs
            SET
                end_time = ?,
                status = ?,
                input_count = ?,
                output_count = ?,
                rejected_count = ?
            WHERE run_id = ?
        """, (
            get_timestamp(),
            "SUCCESS",
            input_count,
            output_count,
            rejected_count,
            run_id
        ))

        connection.commit()

        logger.info(
            "Pipeline completed | Run ID: %s | Input: %s | Applied: %s | Rejected: %s",
            run_id,
            input_count,
            output_count,
            rejected_count
        )

        print("\nPipeline execution successful")
        print("------------------------------")
        print("Run ID:", run_id)
        print("Input records:", input_count)
        print("Applied customer changes:", output_count)
        print("Rejected records:", rejected_count)
        print("Status: SUCCESS")

    except Exception as error:

        connection.rollback()

        cursor.execute("""
            UPDATE pipeline_runs
            SET
                end_time = ?,
                status = ?,
                input_count = ?,
                output_count = ?,
                rejected_count = ?,
                error_message = ?
            WHERE run_id = ?
        """, (
            get_timestamp(),
            "FAILED",
            input_count,
            output_count,
            rejected_count,
            str(error),
            run_id
        ))

        connection.commit()

        logger.exception(
            "Pipeline failed | Run ID: %s",
            run_id
        )

        print("\nPipeline execution failed")
        print("Run ID:", run_id)
        print("Error:", str(error))

        raise

    finally:
        connection.close()


if __name__ == "__main__":
    if __name__ == "__main__":
        large_file = INPUT_DIR / "large_events.csv"


   

    ingest_file(large_file)

    