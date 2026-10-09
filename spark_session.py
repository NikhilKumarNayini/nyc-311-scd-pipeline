from pathlib import Path
from pyspark.sql import SparkSession


def get_spark():

    jar_path = (
            Path(__file__).parent
            / "jars"
            / "gcs-connector-3.0.1-shaded.jar"
    )

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("NYC311Pipeline")
        .config(
            "spark.jars",
            str(jar_path)
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )
        .config(
            "spark.hadoop.fs.gs.auth.type",
            "APPLICATION_DEFAULT"
        )
        .getOrCreate()
    )

    return spark