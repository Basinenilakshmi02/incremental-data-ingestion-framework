
import csv
from itertools import islice

from config import BATCH_SIZE


def read_csv_in_batches(file_path, batch_size=BATCH_SIZE):
    """
    Read a CSV file in smaller batches instead of
    loading the entire file into memory.
    """

    if batch_size <= 0:
        raise ValueError("Batch size must be greater than zero")

    with open(file_path, "r", encoding="utf-8", newline="") as file:

        reader = csv.DictReader(file)

        while True:

            batch = list(islice(reader, batch_size))

            if not batch:
                break

            yield batch
