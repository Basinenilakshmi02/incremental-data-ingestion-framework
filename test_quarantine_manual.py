
import csv
import uuid

from config import INPUT_DIR
from validation import validate_batch

from quarantine import (
    store_rejected_records,
    export_quarantine_records
)


def main():

    file_path = INPUT_DIR / "sample_events.csv"

    with open(file_path, "r", encoding="utf-8") as file:

        records = list(csv.DictReader(file))

    # Validate source records
    valid_records, rejected_records = validate_batch(records)

    # Generate unique pipeline run ID
    run_id = str(uuid.uuid4())

    # Store rejected records
    rejected_count = store_rejected_records(
        rejected_records,
        run_id,
        file_path.name
    )

    # Export quarantine records
    export_quarantine_records()

    print("\nQuarantine Summary")
    print("------------------")

    print("Total Input:", len(records))
    print("Valid Records:", len(valid_records))
    print("Rejected Records:", len(rejected_records))
    print("Newly Stored Rejects:", rejected_count)


if __name__ == "__main__":

    main()
