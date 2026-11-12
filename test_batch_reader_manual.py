
from config import INPUT_DIR, BATCH_SIZE
from batch_reader import read_csv_in_batches

file_path = INPUT_DIR / "large_events.csv"

total_records = 0
total_batches = 0

for batch in read_csv_in_batches(file_path):

    total_batches += 1
    total_records += len(batch)

    print(
        f"Batch {total_batches}: "
        f"{len(batch)} records"
    )

print("\nBATCH PROCESSING SUMMARY")
print("------------------------")
print("Total records:", total_records)
print("Total batches:", total_batches)
print("Batch size:", BATCH_SIZE)
