import json

from google.cloud import storage
from pyspark.sql.functions import (
    col,
    greatest,
    max as spark_max
)


def read_watermark(
        bucket_name,
        watermark_path
):

    client = storage.Client()

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(watermark_path)

    if not blob.exists():
        return None

    watermark_data = json.loads(
        blob.download_as_text()
    )

    return watermark_data["watermark"]


def write_watermark(
        bucket_name,
        watermark_path,
        watermark
):

    client = storage.Client()

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(watermark_path)

    watermark_data = {
        "watermark": watermark
    }

    blob.upload_from_string(
        json.dumps(
            watermark_data,
            indent=2
        ),
        content_type="application/json"
    )

    print(
        f"Watermark updated to: "
        f"{watermark}"
    )

def get_max_source_timestamp(df):

    result = (
        df
        .select(
            greatest(
                col("created_date"),
                col("resolution_action_updated_date")
            ).alias("source_timestamp")
        )
        .agg(
            spark_max(
                "source_timestamp"
            ).alias("watermark")
        )
        .first()
    )

    return result["watermark"]