import pytest
from pyspark.sql import SparkSession
from quality.checks.curated_checks import (
    check_not_null,
    check_uniqueness,
    check_range,
    check_row_count_threshold,
    DataQualityFailure,
)

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_dq")
        .getOrCreate()
    )

def test_check_not_null_passes(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    check_not_null(df, "id", "test_table")

def test_check_not_null_fails(spark):
    df = spark.createDataFrame([(1, "a"), (None, "b")], ["id", "val"])
    with pytest.raises(DataQualityFailure):
        check_not_null(df, "id", "test_table")

def test_check_uniqueness_passes(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
    check_uniqueness(df, "id", "test_table")

def test_check_uniqueness_fails(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["id"])
    with pytest.raises(DataQualityFailure):
        check_uniqueness(df, "id", "test_table")

def test_check_range_passes(spark):
    df = spark.createDataFrame([(5.0,), (10.0,)], ["amount"])
    check_range(df, "amount", 0.0, 100.0, "test_table")

def test_check_range_fails(spark):
    df = spark.createDataFrame([(-1.0,), (10.0,)], ["amount"])
    with pytest.raises(DataQualityFailure):
        check_range(df, "amount", 0.0, 100.0, "test_table")

def test_check_row_count_threshold_passes(spark):
    df = spark.createDataFrame([(i,) for i in range(10)], ["id"])
    check_row_count_threshold(df, min_rows=5, table="test_table")

def test_check_row_count_threshold_fails(spark):
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    with pytest.raises(DataQualityFailure):
        check_row_count_threshold(df, min_rows=10, table="test_table")
