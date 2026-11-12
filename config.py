
from pathlib import Path

# Project root directory
BASE_DIR = Path(__file__).resolve().parent

# Data directories
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
QUARANTINE_DIR = DATA_DIR / "quarantine"

# Logging directory
LOG_DIR = BASE_DIR / "logs"

# Database configuration
DATABASE_PATH = OUTPUT_DIR / "incremental_ingestion.db"

# Pipeline configuration
BATCH_SIZE = 1000
WATERMARK_COLUMN = "updated_at"
PRIMARY_KEY = "customer_id"

# Timestamp configuration
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Create required directories
for directory in [
    INPUT_DIR,
    OUTPUT_DIR,
    QUARANTINE_DIR,
    LOG_DIR
]:
    directory.mkdir(parents=True, exist_ok=True)
