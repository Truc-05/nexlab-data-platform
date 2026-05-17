import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from ingestion.failure_handlers import (
    drop_duplicates,
    quarantine_schema_violations,
    filter_late_arriving_data,
)
from observability.logger import get_logger
from observability.metrics import record_metric, timer

logger = get_logger(__name__)

RAW_ZONE = os.environ.get("RAW_ZONE", "s3a://nexlab-lake/raw")
CURATED_ZONE = os.environ.get("CURATED_ZONE", "s3a://nexlab-lake/curated")

FARE_MIN = 0.0
FARE_MAX = 5000.0
DISTANCE_MAX = 200.0
PASSENGER_MAX = 9

def cast_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
        .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount", F.col("tolls_amount").cast(DoubleType()))
    )

def apply_business_rules(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("fare_amount").between(FARE_MIN, FARE_MAX))
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= DISTANCE_MAX)
        & (F.col("passenger_count") > 0)
        & (F.col("passenger_count") <= PASSENGER_MAX)
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )

def add_derived_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("trip_duration_minutes",
                    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
        .withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month", F.month("tpep_pickup_datetime"))
    )

def run(spark: SparkSession, year: int, month: int):
    raw_path = f"{RAW_ZONE}/yellow_tripdata/year={year}/month={month:02d}/data.parquet"
    output_path = f"{CURATED_ZONE}/silver/yellow_tripdata/year={year}/month={month:02d}"

    with timer("cleansing_job"):
        df = spark.read.parquet(raw_path)
        raw_count = df.count()
        logger.info("cleansing_start", raw_count=raw_count, year=year, month=month)

        df = cast_columns(df)
        df, quarantine = quarantine_schema_violations(df)
        df = filter_late_arriving_data(df, year, month)
        df = drop_duplicates(df, ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"])
        df = apply_business_rules(df)
        df = add_derived_columns(df)

        df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)

        clean_count = df.count()
        record_metric("cleansing_records_in", raw_count, year=year, month=month)
        record_metric("cleansing_records_out", clean_count, year=year, month=month)
        record_metric("cleansing_drop_rate", round(1 - clean_count / raw_count, 4), year=year, month=month)

        logger.info("cleansing_done", output=output_path, clean_count=clean_count)


if __name__ == "__main__":
    from processing.utils.spark_session import create_spark_session
    year = int(os.environ.get("NYC_TLC_YEAR", 2023))
    months = [int(m) for m in os.environ.get("NYC_TLC_MONTHS", "1,2,3").split(",")]
    spark = create_spark_session("cleansing")
    for month in months:
        run(spark, year, month)
    spark.stop()
