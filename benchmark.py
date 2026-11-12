
import time
import sqlite3

from pathlib import Path

import database as database_module
import pipeline as pipeline_module

from config import INPUT_DIR, OUTPUT_DIR


# Separate database for performance testing
BENCHMARK_DATABASE = OUTPUT_DIR / "performance_test.db"

# Large input file
LARGE_FILE = INPUT_DIR / "large_events.csv"


def run_benchmark():

    print("\nStarting Performance Benchmark")
    print("--------------------------------")

    # Check whether input file exists
    if not LARGE_FILE.exists():
        raise FileNotFoundError(
            f"Large input file not found: {LARGE_FILE}"
        )

    # Remove previous benchmark database
    if BENCHMARK_DATABASE.exists():
        BENCHMARK_DATABASE.unlink()

    # Use an isolated database for benchmarking
    database_module.DATABASE_PATH = BENCHMARK_DATABASE
    pipeline_module.DATABASE_PATH = BENCHMARK_DATABASE

    # Create database tables
    database_module.create_tables()

    # Calculate file size
    file_size = LARGE_FILE.stat().st_size
    file_size_mb = file_size / (1024 * 1024)

    print(f"Input file: {LARGE_FILE.name}")
    print(f"File size: {file_size_mb:.2f} MB")

    # Start performance timer
    start_time = time.perf_counter()

    # Execute ingestion pipeline
    pipeline_module.ingest_file(LARGE_FILE)

    # Stop performance timer
    end_time = time.perf_counter()

    execution_time = end_time - start_time

    # Read database metrics
    connection = sqlite3.connect(BENCHMARK_DATABASE)
    cursor = connection.cursor()

    cursor.execute("SELECT COUNT(*) FROM customers")
    customer_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM event_history")
    event_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM rejected_records")
    rejected_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT input_count, output_count, status
        FROM pipeline_runs
        ORDER BY rowid DESC
        LIMIT 1
    """)

    run_details = cursor.fetchone()

    connection.close()

    input_count, output_count, status = run_details

    # Calculate throughput
    throughput = (
        input_count / execution_time
        if execution_time > 0
        else 0
    )

    # Display benchmark results
    print("\nPERFORMANCE RESULTS")
    print("--------------------------------")
    print(f"Input records       : {input_count}")
    print(f"Customer records    : {customer_count}")
    print(f"Events recorded     : {event_count}")
    print(f"Rejected records    : {rejected_count}")
    print(f"Applied changes     : {output_count}")
    print(f"Execution time      : {execution_time:.2f} seconds")
    print(f"Throughput          : {throughput:.2f} records/second")
    print(f"Pipeline status     : {status}")

    print("--------------------------------")
    print("Benchmark completed successfully!")


if __name__ == "__main__":
    run_benchmark()
