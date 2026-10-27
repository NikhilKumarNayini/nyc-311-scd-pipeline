# NYC 311 SCD Pipeline

A Python-based ETL pipeline for ingesting NYC 311 service request data into Google Cloud Storage (GCS) and transforming it into a curated, slowly changing dimension (SCD) dataset with Apache Spark.

This project demonstrates a practical data engineering workflow: pull raw public data, land it in object storage, build an initial analytical snapshot, and apply a watermark-based incremental refresh pattern for daily updates.

## Project overview

New York City 311 service requests are exposed through the NYC Open Data API. This pipeline reads the public source feed, stores raw JSON into GCS, and creates a curated dimension table that can support downstream BI and analytics workloads.

The workflow is designed around the following core ideas:

- ingest raw data from a public API
- persist raw snapshots in cloud storage
- build a curated dimensional model with Spark
- track the latest processed timestamp via a watermark
- incrementally load only new or changed records on subsequent runs

## Architecture

```text
NYC 311 Open Data API
        │
        ▼
   extract_311_page()
   extract_311_incremental_page()
        │
        ▼
     Raw JSON in GCS
        │
        ▼
   Spark Transformation
        │
        ▼
 Curated Parquet Dataset
        │
        ▼
   Watermark (GCS metadata JSON)
        │
        ▼
   Daily incremental refresh
```

## Key features

- Full initial API extraction for historical load
- Incremental daily ingestion using a timestamp watermark
- GCS-based raw storage for JSON files
- Apache Spark transformation for a curated dataset
- Slowly changing dimension style modeling using snapshot logic
- Simple orchestration in Python for ETL execution

## Tech stack

- Python 3
- PySpark
- Google Cloud Storage
- requests
- Spark GCS connector

## Repository structure

```text
.
├── config.py
├── extractor.py
├── loader.py
├── main.py
├── requirements.txt
├── spark_session.py
├── transformer.py
├── watermark.py
├── README.md
└── .gitignore
```

### File purposes

- `main.py` — orchestrates the full historical load and incremental pipeline flow
- `config.py` — stores API URL, GCS bucket names, paths, and watermark settings
- `extractor.py` — handles API requests for full and incremental extraction
- `transformer.py` — applies Spark transformations to generate the curated dimension
- `loader.py` — uploads raw JSON and writes Parquet output to GCS
- `watermark.py` — reads and writes the watermark metadata blob and computes max timestamps
- `spark_session.py` — builds the Spark session with required GCS configuration
- `requirements.txt` — Python dependency list

## Data flow

### 1. Initial raw ingestion

The pipeline begins by calling `extract_311_page()` to retrieve pages of NYC 311 records from the API. Each batch is written to GCS under a raw path such as:

```text
raw/311/initial_load/
```

This allows the raw data to be preserved before any transformations are applied.

### 2. Initial dimension creation

The raw JSON files are read with Spark and passed into `create_initial_dimension()`. The transformation selects a subset of fields, converts timestamp strings to proper Spark timestamp types, and creates a dimension-style dataset with lineage columns such as:

- `request_sk`
- `unique_key`
- `agency`
- `agency_name`
- `complaint_type`
- `descriptor`
- `status`
- `borough`
- `created_date`
- `closed_date`
- `resolution_action_updated_date`
- `effective_start_date`
- `effective_end_date`
- `is_current`

### 3. Watermark initialization

Once the initial curated dataset is built, the project calculates the maximum timestamp from the source fields and stores it in GCS as a JSON watermark. This watermark acts as the lower bound for future incremental loads.

### 4. Daily incremental extraction

Each scheduled run loads the current watermark from GCS and requests only records where:

- `created_date` is greater than the watermark, or
- `resolution_action_updated_date` is greater than the watermark

This prevents reprocessing the full dataset and keeps the ingestion pattern efficient.

## Configuration

The project centralizes operational settings in `config.py`:

- `NYC_311_API_URL` — source endpoint for NYC 311 data
- `RAW_BUCKET_NAME` — bucket used for raw JSON files
- `CURATED_BUCKET_NAME` — bucket used for curated dataset output
- `PAGE_SIZE` — records per API page
- `MAX_RECORDS` — cap for the initial historical load
- `WATERMARK_PATH` — metadata file used to persist the watermark
- `DAILY_RAW_PREFIX` — path prefix for daily raw partitions

## Prerequisites

Before running the pipeline, ensure the following are available:

- Python 3.x installed
- Apache Spark available in the environment
- A Google Cloud project with GCS access configured
- Proper service account or application default credentials configured
- GCS buckets created for raw and curated data
- Spark GCS connector JAR available for the configured runtime

## Setup

1. Clone the repository.
2. Create and activate a virtual environment if desired.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Update `config.py` with your bucket names and environment-specific values.
5. Ensure your Google Cloud credentials are active.
6. Confirm the GCS connector JAR is available in the expected path used by `spark_session.py`.

## Running the pipeline

The orchestration logic lives in `main.py`. The script includes the main phases for:

- initial raw data load
- initial dimension creation
- watermark initialization
- daily incremental extraction

You can run the job with:

```bash
python main.py
```

The script includes commented steps that represent the intended execution order:

```python
# load_initial_raw_data()
# build_initial_dimension()
# initialize_watermark()
# load_daily_raw_data()
```

## Example workflow

A typical operational sequence looks like this:

1. Perform the first historical raw load.
2. Build the initial curated dimension.
3. Initialize the watermark from the max source timestamp.
4. Schedule or execute the incremental daily job.
5. Continue updating the watermark after each successful run.

## Why this project matters

This project is a strong example of a modern data engineering pattern:

- reliable ingestion from a public source
- storage in a cloud data lake
- processing with distributed compute
- incremental processing using watermark logic
- creation of a dimension layer for downstream analytics

It is especially useful for teams building analytics pipelines on top of operational data and wanting to minimize unnecessary reprocessing.

## Notes and considerations

- The pipeline is designed for GCS-backed storage and assumes valid cloud authentication.
- The Spark session configuration expects the GCS connector JAR to be available locally.
- The watermark pattern is intentionally simple and based on timestamp comparisons for incremental freshness.
- The project is best suited for learning, prototyping, and data engineering demonstrations.

## Future improvements

Possible next steps include:

- adding Docker support for easier environment setup
- introducing automated tests for extractor and transformer logic
- externalizing configuration with environment variables
- adding scheduling for daily refresh jobs
- improving SCD handling for more advanced historical tracking
- adding quality checks and validation stages

## Summary

The NYC 311 SCD Pipeline is a clean example of ETL orchestration using Python, Spark, and GCS. It demonstrates how to ingest public open data, create a curated analytical layer, and process incremental updates without re-reading the entire dataset.

