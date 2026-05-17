import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from observability.logger import get_logger
from observability.metrics import record_metric, timer

logger = get_logger(__name__)

RAW_ZONE = os.environ.get("RAW_ZONE", "s3a://nexlab-lake/raw")
CURATED_ZONE = os.environ.get("CURATED_ZONE", "s3a://nexlab-lake/curated")

def load_taxi_zones(spark: SparkSession) -> DataFrame:
    path = f"{RAW_ZONE}/taxi_zones/taxi_zone_lookup.csv"
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .withColumnRenamed("service_zone", "service_zone")
    )

def join_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pickup_zones = zones.select(
        F.col("location_id").alias("PULocationID"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("DOLocationID"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )

    return (
        trips
        .join(pickup_zones, on="PULocationID", how="left")
        .join(dropoff_zones, on="DOLocationID", how="left")
    )

def run(spark: SparkSession, year: int, months: list[int]):
    silver_path = f"{CURATED_ZONE}/silver/yellow_tripdata"
    output_path = f"{CURATED_ZONE}/silver/trips_with_zones/year={year}"

    with timer("join_job"):
        trips = spark.read.parquet(silver_path).filter(F.col("year") == year)
        zones = load_taxi_zones(spark)

        trip_count = trips.count()
        logger.info("join_start", trip_count=trip_count, year=year)

        enriched = join_zones(trips, zones)

        enriched.write.mode("overwrite").partitionBy("month").parquet(output_path)

        record_metric("join_records_out", enriched.count(), year=year)
        logger.info("join_done", output=output_path)


if __name__ == "__main__":
    from processing.utils.spark_session import create_spark_session
    year = int(os.environ.get("NYC_TLC_YEAR", 2023))
    months = [int(m) for m in os.environ.get("NYC_TLC_MONTHS", "1,2,3").split(",")]
    spark = create_spark_session("join_tables")
    run(spark, year, months)
    spark.stop()
