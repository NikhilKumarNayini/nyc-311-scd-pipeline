import json

from google.cloud import storage


def upload_json_to_gcs(
        data,
        bucket_name,
        destination_path
):

    client = storage.Client()

    bucket = client.bucket(bucket_name)

    blob = bucket.blob(destination_path)

    json_data = json.dumps(data)

    blob.upload_from_string(
        json_data,
        content_type="application/json"
    )

    return f"gs://{bucket_name}/{destination_path}"

def write_parquet_to_gcs(
        df,
        gcs_path
):
    (
        df.write
        .mode("overwrite")
        .parquet(gcs_path)
    )

    return gcs_path