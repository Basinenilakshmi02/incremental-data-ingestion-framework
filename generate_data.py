
import csv
import random
from datetime import datetime, timedelta

from config import INPUT_DIR


# Reproducible random data
random.seed(42)

# Source dataset columns
FIELDS = [
    "event_id",
    "customer_id",
    "customer_name",
    "email",
    "city",
    "updated_at",
    "operation"
]


def generate_sample_data():

    records = [
        {
            "event_id": "EVT001",
            "customer_id": 101,
            "customer_name": "Lakshmi",
            "email": "lakshmi@gmail.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-01T09:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "EVT002",
            "customer_id": 102,
            "customer_name": "Priya",
            "email": "priya@gmail.com",
            "city": "Bangalore",
            "updated_at": "2026-11-01T10:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "EVT003",
            "customer_id": 103,
            "customer_name": "Rahul",
            "email": "rahul@gmail.com",
            "city": "Chennai",
            "updated_at": "2026-11-01T11:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "EVT004",
            "customer_id": 101,
            "customer_name": "Lakshmi",
            "email": "lakshmi.new@gmail.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-02T09:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "EVT005",
            "customer_id": 104,
            "customer_name": "Anjali",
            "email": "anjali@gmail.com",
            "city": "Mumbai",
            "updated_at": "2026-11-02T10:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "EVT006",
            "customer_id": 102,
            "customer_name": "Priya",
            "email": "priya@gmail.com",
            "city": "Bangalore",
            "updated_at": "2026-11-03T09:00:00",
            "operation": "DELETE"
        },
        {
            "event_id": "EVT007",
            "customer_id": 105,
            "customer_name": "Sneha",
            "email": "sneha@gmail.com",
            "city": "Hyderabad",
            "updated_at": "2026-11-01T08:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "EVT008",
            "customer_id": 106,
            "customer_name": "",
            "email": "invalid@gmail.com",
            "city": "Delhi",
            "updated_at": "2026-11-03T10:00:00",
            "operation": "UPSERT"
        },
        {
            "event_id": "EVT009",
            "customer_id": 107,
            "customer_name": "Kiran",
            "email": "kiran@gmail.com",
            "city": "Pune",
            "updated_at": "2026-11-03T11:00:00",
            "operation": "INVALID"
        },
        {
            "event_id": "EVT010",
            "customer_id": 0,
            "customer_name": "Test User",
            "email": "test@gmail.com",
            "city": "Delhi",
            "updated_at": "2026-11-03T12:00:00",
            "operation": "UPSERT"
        }
    ]

    # Add a duplicate event intentionally
    records.append(records[0].copy())

    return records


def generate_large_data(total_records=100000):

    records = []

    start_date = datetime(2026, 1, 1)

    cities = [
        "Hyderabad",
        "Bangalore",
        "Chennai",
        "Mumbai",
        "Delhi",
        "Pune"
    ]

    for i in range(1, total_records + 1):

        updated_time = start_date + timedelta(minutes=i)

        record = {
            "event_id": f"EVT{i:07d}",
            "customer_id": i,
            "customer_name": f"Customer_{i}",
            "email": f"customer{i}@example.com",
            "city": random.choice(cities),
            "updated_at": updated_time.strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "operation": "UPSERT"
        }

        records.append(record)

    return records


def save_csv(records, file_name):

    file_path = INPUT_DIR / file_name

    with open(file_path, "w", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(file, fieldnames=FIELDS)

        writer.writeheader()

        writer.writerows(records)

    print(f"File created successfully: {file_path}")
    print(f"Total records: {len(records)}")


if __name__ == "__main__":

    # Generate small sample dataset
    sample_records = generate_sample_data()

    save_csv(sample_records, "sample_events.csv")

    # Generate larger dataset
    large_records = generate_large_data(100000)

    save_csv(large_records, "large_events.csv")

    print("\nData generation completed successfully!")
