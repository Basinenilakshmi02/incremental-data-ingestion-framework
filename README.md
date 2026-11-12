# Incremental Data Ingestion Framework
### A Python-Based Data Engineering Project

**Technologies:** Python | SQLite | SQL | Pandas | Pytest | Logging | CSV

## 1. Project Overview

The Incremental Data Ingestion Framework is a Python-based data engineering solution designed to ingest, validate, process, and maintain customer data from CSV source files into a structured SQLite database.

The framework addresses common challenges in data engineering, including duplicate events, late-arriving records, deletion handling, invalid data, batch processing, and recovery from partial failures.

It follows a modular architecture with separate components for configuration, database management, data validation, ingestion, logging, and reconciliation.

The primary objective is to build a reliable, reusable, and testable ingestion framework that maintains data consistency while processing large datasets.

## 2. Business Problem

In real-world data pipelines, incoming source data frequently contains:

- Duplicate records and repeated events.
- Late-arriving or out-of-order updates.
- Invalid or incomplete records.
- Customer deletion requests.
- Failures during large data-processing jobs.
- Data inconsistencies between source events and target tables.

Processing these records without proper controls can lead to duplicate data, incorrect customer information, and unreliable reporting.

**Solution:** This project implements event tracking, validation, batch transactions, idempotent processing, tombstone-based deletion, watermark tracking, and reconciliation to address these challenges.

## 3. Key Technical Features

### 3.1 Batch Data Processing
- Reads CSV files in configurable batches.
- Uses Python generators to process records incrementally.
- Reduces memory consumption compared with loading the complete dataset into a single list.
- Supports batch-level transaction commits.

### 3.2 Idempotent Data Ingestion
- Maintains a unique event identifier.
- Uses SHA-256 payload hashing to identify event payload changes.
- Prevents previously processed events from being applied again.
- Supports safe reruns.

### 3.3 Late-Arriving Data Handling
- Compares incoming event timestamps with the latest stored customer timestamp.
- Prevents older events from overwriting newer customer information.
- Maintains event history even when an event does not change the current customer state.

### 3.4 Delete and Tombstone Management
- Supports UPSERT and DELETE operations.
- Uses soft deletion instead of physically removing customer records.
- Preserves deletion information through an `is_deleted` flag.

### 3.5 Data Validation and Quarantine
- Validates required fields and data types.
- Checks customer IDs, email formats, timestamps, and operation types.
- Separates invalid records into a rejected-records table.
- Maintains rejection reasons for auditing.

### 3.6 Partial-Failure Recovery
- Uses separate database transactions for individual batches.
- Commits successful batches independently.
- Rolls back the failed batch.
- Uses event history to skip previously committed events during reruns.

### 3.7 Watermark Management
- Maintains source-level processing timestamps.
- Tracks the maximum processed event timestamp for each source file.
- Prevents watermark regression.

### 3.8 Logging and Monitoring
- Uses Python's logging module.
- Records pipeline execution details, batch progress, and errors.
- Uses rotating log files to manage log-file size.
- Maintains execution metrics in the database.

### 3.9 Data Reconciliation
- Compares processed event history with customer records.
- Checks missing customer records.
- Validates the latest customer state.
- Verifies deletion status and source watermarks.

## 4. System Architecture

The framework follows this processing flow:

```text
             CSV SOURCE FILES
                     |
                     v
              BATCH READER
                     |
                     v
              DATA VALIDATION
                     |
              +------+------+
              |             |
              v             v
         VALID DATA     REJECTED DATA
              |             |
              v             v
       EVENT HISTORY    QUARANTINE
              |
              v
       DUPLICATE CHECK
              |
              v
       TIMESTAMP CHECK
              |
         +----+----+
         |         |
         v         v
       UPSERT    DELETE
         |         |
         +----+----+
              |
              v
       CUSTOMER TABLE
              |
              v
       WATERMARK UPDATE
              |
              v
       RECONCILIATION
              |
              v
       EXECUTION REPORT
```

## 5. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Core ingestion and processing logic |
| SQLite | Structured target database |
| SQL | Data manipulation and reconciliation |
| Pandas | Data analysis and supporting data operations |
| CSV | Source data format |
| Pytest | Automated testing |
| Logging | Execution monitoring and error tracking |
| SHA-256 | Event payload integrity checking |

## 6. Database Design

The framework uses the following database tables:

| Table | Responsibility |
|---|---|
| `customers` | Stores the latest customer state |
| `event_history` | Maintains processed event records |
| `watermarks` | Stores source processing timestamps |
| `pipeline_runs` | Tracks pipeline execution status and metrics |
| `rejected_records` | Stores invalid records and rejection reasons |

Indexes are created to support efficient timestamp-based and customer-event lookups.

## 7. Project Structure

```text
incremental_injextion_framework/
│
├── config.py
├── database.py
├── generate_data.py
├── validation.py
├── quarantine.py
├── batch_reader.py
├── pipeline.py
├── logger_config.py
├── benchmark.py
├── reconciliation.py
├── check_output.py
├── requirements.txt
├── README.md
│
├── data/
│   ├── input/
│   ├── output/
│   └── quarantine/
│
├── logs/
│
└── tests/
    ├── test_validation.py
    ├── test_database.py
    ├── test_pipeline.py
    └── test_partial_recovery.py
```

## 8. Installation and Execution

### Prerequisites
- Python 3.10 or above.
- pip package manager.
- Git (optional, for version control).

### Step 1: Clone the repository

```bash
git clone YOUR_GITHUB_REPOSITORY_URL
cd incremental-injextion_framework
```

### Step 2: Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Step 3: Generate sample datasets

```bash
python generate_data.py
```

This generates reproducible sample data, including a large dataset containing 100,000 records.

### Step 4: Execute the ingestion pipeline

```bash
python pipeline.py
```

### Step 5: Run automated tests

```bash
python -m pytest -v
```

### Step 6: Execute performance benchmarking

```bash
python benchmark.py
```

### Step 7: Run data reconciliation

```bash
python reconciliation.py
```

## 9. Performance and Validation Results

The framework was tested using a reproducible dataset containing 100,000 records.

### Large Dataset Processing

| Metric | Observed result |
|---|---:|
| Input records | 100,000 |
| Batch size | 1,000 |
| Number of batches | 100 |
| Successfully applied customer changes | 99,994 |
| Rejected records | 0 |
| Pipeline status | SUCCESS |

Six events did not change the current customer state because they were older than the existing customer data.

### Reconciliation Results

| Validation metric | Result |
|---|---:|
| Processed events | 100,009 |
| Customer records | 100,000 |
| Active customers | 99,998 |
| Deleted customers | 2 |
| Missing customer records | 0 |
| Customer state mismatches | 0 |
| Watermark mismatches | 0 |

**Final reconciliation status: PASSED**

### Automated Testing

- 16 previously verified automated tests passed.
- The additional partial-failure recovery test passed independently.
- Batch processing was verified with 100,000 records.

## 10. Engineering Concepts Demonstrated

This project demonstrates practical understanding of:

- ETL and data ingestion concepts.
- Incremental data processing.
- Batch processing and transaction management.
- Database design and SQL queries.
- Data validation and error handling.
- Event-driven processing concepts.
- Idempotency and duplicate detection.
- Watermark tracking.
- Soft deletes and tombstones.
- Logging and monitoring.
- Automated testing.
- Data quality and reconciliation.
- Performance measurement.

## 11. Future Enhancements

Potential improvements include:

- Implementing watermark-based source extraction to read only newly changed records.
- Supporting multiple source systems and configurable source schemas.
- Adding cloud storage integration using Azure Data Lake Storage.
- Integrating Azure SQL Database.
- Implementing scheduled pipeline execution.
- Adding dashboard-based pipeline monitoring.
- Introducing parallel processing for larger datasets.

## 12. Conclusion

The Incremental Data Ingestion Framework demonstrates how to design and implement a modular data ingestion pipeline with an emphasis on data quality, reliability, recoverability, and consistency.

By combining Python, SQL, SQLite, batch processing, automated testing, and reconciliation, this project provides practical experience with important data engineering principles.

**Developed as a hands-on data engineering project to demonstrate practical Python programming, database management, and reliable data pipeline development.**

---

**Author:** Lakshmi Basineni  
**Domain:** Data Engineering | Python | SQL | Data Pipelines