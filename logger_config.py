import logging
from logging.handlers import RotatingFileHandler

from config import LOG_DIR


def get_logger():

    logger = logging.getLogger("incremental_ingestion")

    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    log_file = LOG_DIR / "ingestion.log"

    # Save logs into a file with automatic rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )

    # Display logs in the terminal
    console_handler = logging.StreamHandler()

    # Define log format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
