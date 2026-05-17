import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from observability.logger import get_logger
from observability.metrics import record_metric, timer

logger = get_logger(__name__)

CURATED_ZONE = os.environ.get("CURATED_ZONE", "s3a://nexlab-lake/curated")

def hourly_aggregation(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_date", "pickup_hour", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("fare_amount").alias("total_fare"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_minutes").alias("avg_duration_minutes"),
            F.sum("tip_amount").alias("total_tips"),
        )
    )

def daily_aggregation(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("pickup_date", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("revenue"),
            F.avg("trip_distance").alias("avg_distance"),
            F.countDistinct("PULocationID").alias("unique_pickup_zones"),
        )
    )

def rolling_7day_revenue(daily: DataFrame) -> DataFrame:
    window = (
        Window
        .partitionBy("pickup_borough")
        .orderBy(F.col("pickup_date").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    return daily.withColumn("rolling_7d_revenue", F.sum("revenue").over(window))

def run(spark: SparkSession, year: int):
    input_path = f"{CURATED_ZONE}/silver/trips_with_zones/year={year}"
    hourly_output = f"{CURATED_ZONE}/gold/hourly_stats/year={year}"
    daily_output = f"{CURATED_ZONE}/gold/daily_stats/year={year}"

    with timer("windowed_agg_job"):
        df = spark.read.parquet(input_path)
        count = df.count()
        logger.info("agg_start", record_count=count, year=year)

        hourly = hourly_aggregation(df)
        hourly.write.mode("overwrite").partitionBy("pickup_date").parquet(hourly_output)

        daily = daily_aggregation(df)
        daily_with_rolling = rolling_7day_revenue(daily)
        daily_with_rolling.write.mode("overwrite").partitionBy("pickup_date").parquet(daily_output)

        record_metric("agg_hourly_rows", hourly.count(), year=year)
        record_metric("agg_daily_rows", daily.count(), year=year)
        logger.info("agg_done", hourly_output=hourly_output, daily_output=daily_output)


if __name__ == "__main__":
    from processing.utils.spark_session import create_spark_session
    year = int(os.environ.get("NYC_TLC_YEAR", 2023))
    spark = create_spark_session("windowed_agg")
    run(spark, year)
    spark.stop()
