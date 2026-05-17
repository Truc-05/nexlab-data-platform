from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from observability.logger import get_logger

logger = get_logger(__name__)

REQUIRED_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
}


def drop_duplicates(df: DataFrame, dedup_keys: list[str]) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates(dedup_keys)
    after = df.count()
    logger.info("dedup", removed=before - after, remaining=after)
    return df


def quarantine_schema_violations(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        logger.warning("schema_missing_columns", columns=list(missing))
        empty = df.limit(0)
        return empty, df

    null_condition = F.lit(False)
    for col in REQUIRED_COLUMNS:
        null_condition = null_condition | F.col(col).isNull()

    bad = df.filter(null_condition)
    good = df.filter(~null_condition)

    bad_count = bad.count()
    if bad_count > 0:
        logger.warning("schema_violations_quarantined", count=bad_count)

    return good, bad


def filter_late_arriving_data(df: DataFrame, year: int, month: int) -> DataFrame:
    from calendar import monthrange

    _, last_day = monthrange(year, month)
    period_start = f"{year}-{month:02d}-01"
    period_end = f"{year}-{month:02d}-{last_day}"

    late_condition = (F.col("tpep_pickup_datetime") < period_start) | (
        F.col("tpep_pickup_datetime") > period_end
    )

    late_count = df.filter(late_condition).count()
    if late_count > 0:
        logger.warning("late_arriving_records_dropped", count=late_count, year=year, month=month)

    return df.filter(~late_condition)
