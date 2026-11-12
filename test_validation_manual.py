import csv

from config import INPUT_DIR
from validation import validate_batch


file_path = INPUT_DIR / "sample_events.csv"

with open(file_path, "r", encoding="utf-8") as file:

    reader = csv.DictReader(file)

    records = list(reader)


valid_records, rejected_records = validate_batch(records)

print("Total Input Records:", len(records))

print("Valid Records:", len(valid_records))

print("Rejected Records:", len(rejected_records))

print("\nRejected Record Details:")

for rejected in rejected_records:

    print(
        rejected["record"]["event_id"],
        "->",
        rejected["reason"]
    )