from datetime import datetime
from zoneinfo import ZoneInfo

from config import (
    NYC_311_API_URL,
    RAW_BUCKET_NAME,
    RAW_INITIAL_LOAD_PATH,
    CURATED_DIM_PATH,
    PAGE_SIZE,
    MAX_RECORDS,
    WATERMARK_PATH,
    DAILY_RAW_PREFIX
)

from extractor import (
    extract_311_page,
    extract_311_incremental_page
)

from spark_session import get_spark

from transformer import create_initial_dimension

from loader import (
    upload_json_to_gcs,
    write_parquet_to_gcs
)

from watermark import (
    read_watermark,
    write_watermark,
    get_max_source_timestamp
)


# ==========================================================
# INITIAL RAW LOAD
# ==========================================================
def load_initial_raw_data():

    offset = 0

    while offset < MAX_RECORDS:

        remaining = MAX_RECORDS - offset
        limit = min(PAGE_SIZE, remaining)

        data = extract_311_page(
            NYC_311_API_URL,
            limit,
            offset
        )

        if not data:
            break

        file_name = (
            f"311_{offset}_"
            f"{offset + len(data) - 1}.json"
        )

        destination_path = (
            f"raw/311/initial_load/"
            f"{file_name}"
        )

        upload_json_to_gcs(
            data,
            RAW_BUCKET_NAME,
            destination_path
        )

        print(
            f"Uploaded {len(data)} records"
        )

        offset += len(data)

        if len(data) < limit:
            break


# ==========================================================
# INITIAL DIMENSION
# ==========================================================
def build_initial_dimension():

    spark = get_spark()

    source_df = (
        spark.read
        .option("multiline", True)
        .json(RAW_INITIAL_LOAD_PATH)
    )

    print(
        f"Raw count: "
        f"{source_df.count()}"
    )

    initial_dim_df = (
        create_initial_dimension(
            source_df
        )
    )

    print(
        f"Dimension count: "
        f"{initial_dim_df.count()}"
    )

    write_parquet_to_gcs(
        initial_dim_df,
        CURATED_DIM_PATH
    )

    spark.stop()


# ==========================================================
# INITIAL WATERMARK
# ==========================================================
def initialize_watermark():

    spark = get_spark()

    df = spark.read.parquet(
        CURATED_DIM_PATH
    )

    max_timestamp = (
        get_max_source_timestamp(df)
    )

    if max_timestamp is None:

        raise ValueError(
            "Unable to determine watermark."
        )

    watermark = (
        max_timestamp
        .strftime("%Y-%m-%dT%H:%M:%S")
    )

    write_watermark(
        RAW_BUCKET_NAME,
        WATERMARK_PATH,
        watermark
    )

    spark.stop()


# ==========================================================
# DAILY API -> RAW GCS
# ==========================================================
def load_daily_raw_data():

    watermark = read_watermark(
        RAW_BUCKET_NAME,
        WATERMARK_PATH
    )

    if watermark is None:
        raise ValueError(
            "Watermark does not exist."
        )

    upper_watermark = (
        datetime.now(ZoneInfo("America/New_York"))
        .strftime("%Y-%m-%dT%H:%M:%S")
    )

    print(f"Lower watermark: {watermark}")
    print(f"Upper watermark: {upper_watermark}")

    offset = 0

    run_time = datetime.now(
        ZoneInfo("America/New_York")
    )

    run_id = run_time.strftime(
        "%Y%m%d_%H%M%S"
    )

    load_date = run_time.strftime(
        "%Y-%m-%d"
    )

    total_records = 0

    while True:

        data = extract_311_incremental_page(
            NYC_311_API_URL,
            watermark,
            upper_watermark,
            PAGE_SIZE,
            offset
        )

        if not data:
            break

        file_name = (
            f"311_{offset}_"
            f"{offset + len(data) - 1}.json"
        )

        destination_path = (
            f"{DAILY_RAW_PREFIX}/"
            f"load_date={load_date}/"
            f"run_id={run_id}/"
            f"{file_name}"
        )

        gcs_path = upload_json_to_gcs(
            data,
            RAW_BUCKET_NAME,
            destination_path
        )

        print(
            f"Uploaded {len(data)} records "
            f"to {gcs_path}"
        )

        total_records += len(data)
        offset += len(data)

        if len(data) < PAGE_SIZE:
            break

    print(
        f"Daily records extracted: "
        f"{total_records}"
    )


def main():

    # Already completed:
    #
    # load_initial_raw_data()
    # build_initial_dimension()

    # Run ONCE now:
    # initialize_watermark()

    # We will use this afterward:
    #
    load_initial_raw_data()


if __name__ == "__main__":
    main()