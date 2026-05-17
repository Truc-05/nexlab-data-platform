import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from observability.logger import get_logger
from observability.metrics import record_metric

logger = get_logger(__name__)

CURATED_ZONE = os.environ.get("CURATED_ZONE", "s3a://nexlab-lake/curated")

GRAIN = "one row per completed trip"
PRIMARY_KEY = "trip_id (VendorID + tpep_pickup_datetime + PULocationID)"


def build_fact_trips(spark: SparkSession, year: int) -> DataFrame:
    trips = spark.read.parquet(f"{CURATED_ZONE}/silver/trips_with_zones/year={year}")

    dim_location = spark.read.parquet(f"{CURATED_ZONE}/gold/dim_location")
    pickup_sk = dim_location.select(
        F.col("location_id").alias("PULocationID"),
        F.col("location_sk").alias("pickup_location_sk"),
    )
    dropoff_sk = dim_location.select(
        F.col("location_id").alias("DOLocationID"),
        F.col("location_sk").alias("dropoff_location_sk"),
    )

    dim_date = spark.read.parquet(f"{CURATED_ZONE}/gold/dim_date/year={year}")
    date_sk = dim_date.select(
        F.col("full_date").alias("pickup_date"),
        F.col("date_sk").alias("pickup_date_sk"),
    )

    fact = (
        trips.join(pickup_sk, on="PULocationID", how="left")
        .join(dropoff_sk, on="DOLocationID", how="left")
        .join(date_sk, on="pickup_date", how="left")
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("VendorID").cast("string"),
                    F.col("tpep_pickup_datetime").cast("string"),
                    F.col("PULocationID").cast("string"),
                ),
                256,
            ),
        )
        .select(
            "trip_id",
            "pickup_date_sk",
            "pickup_location_sk",
            "dropoff_location_sk",
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "trip_duration_minutes",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "pickup_hour",
            "year",
            "month",
        )
    )

    return fact


def run(spark: SparkSession, year: int):
    output_path = f"{CURATED_ZONE}/gold/fact_trips/year={year}"

    fact = build_fact_trips(spark, year)
    fact.write.mode("overwrite").partitionBy("month").parquet(output_path)

    count = fact.count()
    record_metric("fact_trips_rows", count, year=year)
    logger.info("fact_trips_written", output=output_path, grain=GRAIN, pk=PRIMARY_KEY, count=count)


if __name__ == "__main__":
    from processing.utils.spark_session import create_spark_session

    year = int(os.environ.get("NYC_TLC_YEAR", 2023))
    spark = create_spark_session("fact_tables")
    run(spark, year)
    spark.stop()
