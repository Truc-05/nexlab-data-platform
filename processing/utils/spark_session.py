import os
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = None) -> SparkSession:
    name = app_name or os.environ.get("SPARK_APP_NAME", "nexlab-pipeline")
    master = os.environ.get("SPARK_MASTER", "local[*]")
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]

    return (
        SparkSession.builder.appName(name)
        .master(master)
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "io.delta:delta-core_2.12:2.4.0",
        )
        .getOrCreate()
    )
