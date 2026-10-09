from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_date,
    lit,
    row_number,
    to_timestamp
)
from pyspark.sql.window import Window


def create_initial_dimension(source_df: DataFrame) -> DataFrame:

    selected_df = (
        source_df
        .select(
            col("unique_key"),
            col("agency"),
            col("agency_name"),
            col("complaint_type"),
            col("descriptor"),
            col("status"),
            col("borough"),
            col("resolution_description"),
            col("created_date"),
            col("closed_date"),
            col("resolution_action_updated_date")
        )
    )

    cleaned_df = (
        selected_df
        .withColumn(
            "created_date",
            to_timestamp(col("created_date"))
        )
        .withColumn(
            "closed_date",
            to_timestamp(col("closed_date"))
        )
        .withColumn(
            "resolution_action_updated_date",
            to_timestamp(col("resolution_action_updated_date"))
        )
    )

    window_spec = Window.orderBy("unique_key")

    initial_dim_df = (
        cleaned_df
        .withColumn(
            "request_sk",
            row_number().over(window_spec)
        )
        .withColumn(
            "effective_start_date",
            current_date()
        )
        .withColumn(
            "effective_end_date",
            lit(date(9999, 12, 31))
        )
        .withColumn(
            "is_current",
            lit("Y")
        )
        .select(
            "request_sk",
            "unique_key",
            "agency",
            "agency_name",
            "complaint_type",
            "descriptor",
            "status",
            "borough",
            "resolution_description",
            "created_date",
            "closed_date",
            "resolution_action_updated_date",
            "effective_start_date",
            "effective_end_date",
            "is_current"
        )
    )

    return initial_dim_df