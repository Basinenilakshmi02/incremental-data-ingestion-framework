
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from validation import validate_record, validate_batch

def get_valid_record():
    return {
        "event_id": "TEST001",
        "customer_id": "101",
        "customer_name": "Lakshmi",
        "email": "lakshmi@example.com",
        "city": "Hyderabad",
        "updated_at": "2026-11-01T10:00:00",
        "operation": "UPSERT"
    }


def test_valid_record():
    record = get_valid_record()

    is_valid, reason = validate_record(record)

    assert is_valid is True
    assert reason is None


def test_invalid_customer_id():
    record = get_valid_record()
    record["customer_id"] = "-10"

    is_valid, reason = validate_record(record)

    assert is_valid is False
    assert reason == "Customer ID must be positive"


def test_invalid_email():
    record = get_valid_record()
    record["email"] = "invalid-email"

    is_valid, reason = validate_record(record)

    assert is_valid is False
    assert reason == "Invalid email format"


def test_invalid_operation():
    record = get_valid_record()
    record["operation"] = "UPDATE"

    is_valid, reason = validate_record(record)

    assert is_valid is False
    assert reason == "Invalid operation type"


def test_duplicate_event_ids():
    record1 = get_valid_record()
    record2 = get_valid_record()

    valid_records, rejected_records = validate_batch(
        [record1, record2]
    )

    assert len(valid_records) == 1
    assert len(rejected_records) == 1
    assert rejected_records[0]["reason"] == "Duplicate event ID"


def test_valid_delete_record():
    record = get_valid_record()
    record["operation"] = "DELETE"

    is_valid, reason = validate_record(record)

    assert is_valid is True
    assert reason is None
