import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
from ingestion.failure_handlers import drop_duplicates, quarantine_schema_violations, filter_late_arriving_data

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_failure_handlers")
        .getOrCreate()
    )

def make_full_df(spark, rows):
    schema = StructType([
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
    ])
    return spark.createDataFrame(rows, schema)

def test_drop_duplicates_removes_exact_dupes(spark):
    t = datetime(2023, 1, 1, 8, 0)
    rows = [
        (1, t, datetime(2023, 1, 1, 8, 30), 2, 3.5, 10, 20, 12.5, 15.0),
        (1, t, datetime(2023, 1, 1, 8, 30), 2, 3.5, 10, 20, 12.5, 15.0),
        (2, t, datetime(2023, 1, 1, 8, 45), 1, 2.0, 10, 20, 8.0, 10.0),
    ]
    df = make_full_df(spark, rows)
    result = drop_duplicates(df, ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"])
    assert result.count() == 2

def test_quarantine_separates_null_rows(spark):
    rows = [
        (1, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), 2, 3.5, 10, 20, 12.5, 15.0),
        (None, datetime(2023, 1, 1, 9, 0), datetime(2023, 1, 1, 9, 30), 2, 3.5, 10, 20, 12.5, 15.0),
    ]
    df = make_full_df(spark, rows)
    good, bad = quarantine_schema_violations(df)
    assert good.count() == 1
    assert bad.count() == 1

def test_filter_late_arriving_data(spark):
    rows = [
        (1, datetime(2023, 1, 15, 8, 0), datetime(2023, 1, 15, 8, 30), 2, 3.5, 10, 20, 12.5, 15.0),
        (1, datetime(2022, 12, 31, 8, 0), datetime(2022, 12, 31, 8, 30), 2, 3.5, 10, 20, 12.5, 15.0),
    ]
    df = make_full_df(spark, rows)
    result = filter_late_arriving_data(df, year=2023, month=1)
    assert result.count() == 1
