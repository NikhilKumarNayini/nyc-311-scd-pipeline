#NYC URL API
NYC_311_API_URL             = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

#GCS raw bucket storage
RAW_BUCKET_NAME             = "nikhil-nyc311-raw-data"

#GCS curated bucket storage
CURATED_BUCKET_NAME         = "nikhil-nyc311-curated-data"

RAW_INITIAL_LOAD_PATH       = f"gs://{RAW_BUCKET_NAME}/raw/311/initial_load/*.json"

CURATED_DIM_PATH            = (
                                f"gs://{CURATED_BUCKET_NAME}/"
                                "dim_service_request/current_snapshot/"
                                )

PAGE_SIZE                   = 5000
MAX_RECORDS                 = 100000

# Incremental / CDC configuration
WATERMARK_PATH              = "metadata/nyc311_watermark.json"

DAILY_RAW_PREFIX            = "raw/311/daily"

