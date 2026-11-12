
import re
from datetime import datetime


REQUIRED_FIELDS = [
    "event_id",
    "customer_id",
    "customer_name",
    "email",
    "city",
    "updated_at",
    "operation"
]


def validate_record(record):
    """
    Validate a single incoming source record.

    Returns:
        (True, None) for valid records.
        (False, reason) for invalid records.
    """

    # Check required fields
    for field in REQUIRED_FIELDS:

        if field not in record:
            return False, f"Missing required field: {field}"

        if record[field] is None:
            return False, f"Null value in field: {field}"

    # Validate event ID
    if not str(record["event_id"]).strip():
        return False, "Event ID cannot be empty"

    # Validate customer ID
    try:
        customer_id = int(record["customer_id"])

        if customer_id <= 0:
            return False, "Customer ID must be positive"

    except (ValueError, TypeError):
        return False, "Invalid customer ID"

    # Validate operation
    operation = str(record["operation"]).strip().upper()

    if operation not in ["UPSERT", "DELETE"]:
        return False, "Invalid operation type"

    # Validate customer name
    if operation == "UPSERT":

        if not str(record["customer_name"]).strip():
            return False, "Customer name cannot be empty"

        # Validate email
        email = str(record["email"]).strip()

        email_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

        if not re.match(email_pattern, email):
            return False, "Invalid email format"

        # Validate city
        if not str(record["city"]).strip():
            return False, "City cannot be empty"

    # Validate timestamp
    try:

        datetime.strptime(
            record["updated_at"],
            "%Y-%m-%dT%H:%M:%S"
        )

    except (ValueError, TypeError):
        return False, "Invalid timestamp format"

    return True, None


def validate_batch(records):
    """
    Validate multiple source records.

    Identifies duplicate event IDs within the batch.
    """

    valid_records = []
    rejected_records = []

    seen_event_ids = set()

    for record in records:

        is_valid, reason = validate_record(record)

        if not is_valid:

            rejected_records.append({
                "record": record,
                "reason": reason
            })

            continue

        event_id = str(record["event_id"]).strip()

        if event_id in seen_event_ids:

            rejected_records.append({
                "record": record,
                "reason": "Duplicate event ID"
            })

            continue

        seen_event_ids.add(event_id)

        valid_records.append(record)

    return valid_records, rejected_records


if __name__ == "__main__":

    print("Validation module executed successfully!")
