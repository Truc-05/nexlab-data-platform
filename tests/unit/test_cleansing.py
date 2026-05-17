import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
from processing.jobs.cleansing import cast_columns, apply_business_rules, add_derived_columns

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_cleansing")
        .getOrCreate()
    )

def make_trip_df(spark, rows):
    schema = StructType([
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", StringType()),
        StructField("trip_distance", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("fare_amount", StringType()),
        StructField("total_amount", StringType()),
        StructField("tip_amount", StringType()),
        StructField("tolls_amount", StringType()),
    ])
    return spark.createDataFrame(rows, schema)

def test_cast_columns_converts_strings_to_numeric(spark):
    rows = [(1, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), "2", "3.5", 10, 20, "12.5", "15.0", "1.0", "0.0")]
    df = make_trip_df(spark, rows)
    result = cast_columns(df)
    assert dict(result.dtypes)["fare_amount"] == "double"
    assert dict(result.dtypes)["passenger_count"] == "int"

def test_apply_business_rules_removes_zero_distance(spark):
    rows = [
        (1, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), "2", "0.0", 10, 20, "12.5", "15.0", "1.0", "0.0"),
        (1, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), "2", "3.5", 10, 20, "12.5", "15.0", "1.0", "0.0"),
    ]
    df = make_trip_df(spark, rows)
    df = cast_columns(df)
    result = apply_business_rules(df)
    assert result.count() == 1

def test_apply_business_rules_removes_negative_fare(spark):
    rows = [(1, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), "2", "3.5", 10, 20, "-5.0", "15.0", "1.0", "0.0")]
    df = make_trip_df(spark, rows)
    df = cast_columns(df)
    result = apply_business_rules(df)
    assert result.count() == 0

def test_add_derived_columns_creates_pickup_hour(spark):
    rows = [(1, datetime(2023, 1, 15, 14, 30), datetime(2023, 1, 15, 15, 0), "2", "3.5", 10, 20, "12.5", "15.0", "1.0", "0.0")]
    df = make_trip_df(spark, rows)
    df = cast_columns(df)
    df = apply_business_rules(df)
    result = add_derived_columns(df)
    row = result.first()
    assert row["pickup_hour"] == 14
    assert row["year"] == 2023
    assert row["month"] == 1

def test_add_derived_columns_trip_duration(spark):
    rows = [(1, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), "2", "3.5", 10, 20, "12.5", "15.0", "1.0", "0.0")]
    df = make_trip_df(spark, rows)
    df = cast_columns(df)
    df = apply_business_rules(df)
    result = add_derived_columns(df)
    row = result.first()
    assert abs(row["trip_duration_minutes"] - 30.0) < 0.01
