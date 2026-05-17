import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from observability.logger import get_logger

logger = get_logger(__name__)

RAW_ZONE = os.environ.get("RAW_ZONE", "s3a://nexlab-lake/raw")
CURATED_ZONE = os.environ.get("CURATED_ZONE", "s3a://nexlab-lake/curated")


def build_dim_location(spark: SparkSession) -> DataFrame:
    zones = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_ZONE}/taxi_zones/taxi_zone_lookup.csv")
    )

    return (
        zones.withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .withColumnRenamed("service_zone", "service_zone")
        .withColumn("location_sk", F.col("location_id"))
        .select("location_sk", "location_id", "borough", "zone", "service_zone")
    )


def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    date_range = spark.sql(
        f"SELECT sequence(to_date('{start}'), to_date('{end}'), interval 1 day) AS date_array"
    )
    dates = date_range.select(F.explode("date_array").alias("full_date"))

    return (
        dates.withColumn("date_sk", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn(
            "is_weekend", F.when(F.dayofweek("full_date").isin([1, 7]), True).otherwise(False)
        )
        .withColumn("quarter", F.quarter("full_date"))
    )


def run(spark: SparkSession, year: int):
    dim_location = build_dim_location(spark)
    dim_location.write.mode("overwrite").parquet(f"{CURATED_ZONE}/gold/dim_location")
    logger.info("dim_location_written", count=dim_location.count())

    dim_date = build_dim_date(spark, f"{year}-01-01", f"{year}-12-31")
    dim_date.write.mode("overwrite").parquet(f"{CURATED_ZONE}/gold/dim_date/year={year}")
    logger.info("dim_date_written", count=dim_date.count(), year=year)


if __name__ == "__main__":
    from processing.utils.spark_session import create_spark_session

    year = int(os.environ.get("NYC_TLC_YEAR", 2023))
    spark = create_spark_session("dim_tables")
    run(spark, year)
    spark.stop()
