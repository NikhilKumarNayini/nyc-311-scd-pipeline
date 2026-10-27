# NYC 311 SCD Pipeline

This repository implements a Python-based batch data pipeline for ingesting NYC 311 service request data into Google Cloud Storage (GCS) and building a slowly changing dimension (SCD) dataset in Apache Spark.

The project loads the public NYC 311 API into raw JSON storage, creates an initial curated dimension snapshot, and then uses a watermark-based incremental pattern to process daily updates.

## What this pipeline does

- Extracts data from the NYC 311 Open Data API
- Stores raw records as JSON in a GCS bucket
- Builds an initial curated dimension table from the raw data
- Tracks the latest source timestamp using a GCS watermark file
- Loads only new or changed records on subsequent runs
- Uses Apache Spark for transformation and Parquet output

## Project structure

- `main.py` - orchestration logic for initial load and daily incremental load
- `config.py` - environment-specific configuration values for the NYC API, bucket names, and paths
- `extractor.py` - API extraction logic for the full initial load and incremental extraction
- `transformer.py` - Spark transformation logic used to create the initial service request dimension
- `loader.py` - uploads raw JSON and writes Parquet files to GCS
- `watermark.py` - reads/writes the watermark JSON blob and computes the max source timestamp
- `spark_session.py` - creates the Spark session configured for the GCS connector
- `requirements.txt` - Python package dependencies

## Architecture overview

The repository follows a simple medallion-style ingestion pattern:

1. Raw JSON ingestion from NYC 311 API
2. Initial snapshot load to GCS
3. Spark transformation into a curated dimension table
4. Watermark tracking for incremental daily loads
5. Repeated incremental extraction based on `created_date` and `resolution_action_updated_date`

The core idea is to keep the latest valid timestamp in a watermark file stored in GCS and only fetch records newer than that watermark.

## Key data flow

- Initial load:
  - `extract_311_page()` pulls pages of records from the API
  - `upload_json_to_gcs()` writes each page as JSON in `raw/311/initial_load/`
  - `build_initial_dimension()` reads the raw JSON files with Spark
  - `create_initial_dimension()` creates a curated dimension dataset
  - `write_parquet_to_gcs()` writes output to the curated GCS path

- Incremental daily load:
  - `read_watermark()` fetches the current watermark JSON from GCS
  - `extract_311_incremental_page()` queries records where either timestamp is greater than the watermark
  - Each page is uploaded to a dated partition under `raw/311/daily/`
  - The pipeline is designed to continue processing daily changes using the latest watermark value

## Configuration

The project uses `config.py` for all operational settings. Important values include:

- `NYC_311_API_URL`: NYC Open Data endpoint
- `RAW_BUCKET_NAME`: raw JSON bucket used for ingestion
- `CURATED_BUCKET_NAME`: curated bucket used for Parquet output
- `PAGE_SIZE`: number of records per API page
- `MAX_RECORDS`: max rows for the initial full load
- `WATERMARK_PATH`: metadata blob path used for watermark persistence
- `DAILY_RAW_PREFIX`: path prefix for daily raw loads

## Requirements

This project expects the following:

- Python 3.x
- Apache Spark
- Google Cloud project with authenticated access
- GCS buckets created for raw and curated data
- Google Cloud credentials available via `gcloud auth application-default login` or equivalent service account configuration
- The Spark GCS connector JAR file available at `jars/gcs-connector-3.0.1-shaded.jar`

## Python dependencies

Install dependencies from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Dependencies include:

- `requests`
- `google-cloud-storage`
- `pyspark==3.5.9`

## Setup

1. Create the required GCS buckets:
   - raw bucket (for JSON files)
   - curated bucket (for Parquet output)

2. Set the bucket names in `config.py`.

3. Ensure your Google Cloud default credentials are configured.

4. Confirm the Spark GCS connector JAR is available in the repository or your environment.

5. Run the pipeline:

```bash
python main.py
```

## Running the pipeline

The main orchestrator script contains these phases:

- initial raw load
- initial dimension build
- watermark initialization
- daily incremental load

The script currently has these steps commented as operational guidance:

```python
# load_initial_raw_data()
# build_initial_dimension()
# initialize_watermark()
# load_daily_raw_data()
```

In practice, the pipeline is meant to be run in the order needed for your environment: bootstrap the initial load, initialize the watermark, then run recurring incremental loads.

## Important notes

- The pipeline is designed for a GCS-backed data lake pattern rather than a database-centric warehouse.
- It is built around the NYC 311 service request data model and uses `unique_key` as a key identifier.
- The transformation logic creates an SCD-style dimension with columns such as:
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

## Example use cases

This project is useful for:

- daily operational monitoring of NYC 311 complaints
- historical trend analysis by borough, agency, or issue type
- building a slowly changing dimension for downstream analytics
- tracking service-level changes over time using timestamp-based watermarking

## Limitations / considerations

- The project assumes access to GCS and valid credentials in the execution environment.
- The pipeline is built for a single local Spark session and may need adjustments for distributed execution in production.
- The watermark logic is intentionally simple and uses the latest `created_date` / `resolution_action_updated_date` as the threshold for incremental fetches.
- The repo does not appear to include the full Spark connector JAR, so that dependency should be provided before runtime.

## Future enhancements

Potential improvements include:

- adding Docker or Compose support for easier environment setup
- adding unit tests for extraction, transformation, and watermark logic
- creating a proper config management layer using environment variables
- adding a scheduler for daily incremental jobs
- implementing more advanced SCD Type 2 handling and audit columns

## Summary

This repository is a data engineering pipeline for ingesting NYC 311 data into GCS and transforming it into a curated, SCD-style service request dimension. It is a practical example of ETL orchestration using Python, Spark, and cloud storage with incremental processing based on a watermark.

