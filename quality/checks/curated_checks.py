import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from observability.logger import get_logger
from observability.metrics import record_metric

logger = get_logger(__name__)

CURATED_ZONE = os.environ.get("CURATED_ZONE", "s3a://nexlab-lake/curated")


class DataQualityFailure(Exception):
    pass


def check_not_null(df: DataFrame, column: str, table: str):
    null_count = df.filter(F.col(column).isNull()).count()
    record_metric("dq_null_count", null_count, table=table, column=column)
    if null_count > 0:
        raise DataQualityFailure(f"[{table}] column '{column}' has {null_count} null values")
    logger.info("dq_pass_not_null", table=table, column=column)


def check_uniqueness(df: DataFrame, column: str, table: str):
    total = df.count()
    distinct = df.select(column).distinct().count()
    duplicate_count = total - distinct
    record_metric("dq_duplicate_count", duplicate_count, table=table, column=column)
    if duplicate_count > 0:
        raise DataQualityFailure(
            f"[{table}] column '{column}' has {duplicate_count} duplicate values"
        )
    logger.info("dq_pass_uniqueness", table=table, column=column)


def check_range(df: DataFrame, column: str, min_val: float, max_val: float, table: str):
    out_of_range = df.filter(~F.col(column).between(min_val, max_val)).count()
    record_metric("dq_out_of_range", out_of_range, table=table, column=column)
    if out_of_range > 0:
        raise DataQualityFailure(
            f"[{table}] column '{column}' has {out_of_range} out-of-range values"
        )
    logger.info("dq_pass_range", table=table, column=column, min=min_val, max=max_val)


def check_referential_integrity(
    fact: DataFrame, dim: DataFrame, fact_col: str, dim_col: str, table: str
):
    dim_keys = dim.select(dim_col).distinct()
    orphans = fact.join(dim_keys, fact[fact_col] == dim_keys[dim_col], "left_anti").count()
    record_metric("dq_referential_orphans", orphans, table=table, fk=fact_col)
    if orphans > 0:
        logger.warning(
            "dq_warn_referential_integrity", table=table, fk=fact_col, orphan_count=orphans
        )
    else:
        logger.info("dq_pass_referential_integrity", table=table, fk=fact_col)


def check_row_count_threshold(df: DataFrame, min_rows: int, table: str):
    count = df.count()
    record_metric("dq_row_count", count, table=table)
    if count < min_rows:
        raise DataQualityFailure(f"[{table}] has only {count} rows, expected >= {min_rows}")
    logger.info("dq_pass_row_count", table=table, count=count, min_rows=min_rows)


def run(spark: SparkSession, year: int, month: int):
    fact_path = f"{CURATED_ZONE}/gold/fact_trips/year={year}"
    dim_location_path = f"{CURATED_ZONE}/gold/dim_location"

    fact = spark.read.parquet(fact_path).filter(F.col("month") == month)
    dim_location = spark.read.parquet(dim_location_path)

    check_not_null(fact, "trip_id", "fact_trips")
    check_not_null(fact, "tpep_pickup_datetime", "fact_trips")
    check_uniqueness(fact, "trip_id", "fact_trips")
    check_range(fact, "fare_amount", 0.0, 5000.0, "fact_trips")
    check_range(fact, "trip_distance", 0.0, 200.0, "fact_trips")
    check_referential_integrity(
        fact, dim_location, "pickup_location_sk", "location_sk", "fact_trips"
    )
    check_row_count_threshold(fact, min_rows=100_000, table="fact_trips")

    logger.info("all_dq_checks_passed", year=year, month=month)


if __name__ == "__main__":
    from processing.utils.spark_session import create_spark_session

    year = int(os.environ.get("NYC_TLC_YEAR", 2023))
    months = [int(m) for m in os.environ.get("NYC_TLC_MONTHS", "1,2,3").split(",")]
    spark = create_spark_session("data_quality")
    for month in months:
        run(spark, year, month)
    spark.stop()
