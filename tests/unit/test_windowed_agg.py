import pytest
from datetime import date
from pyspark.sql import SparkSession
from processing.jobs.windowed_agg import hourly_aggregation, daily_aggregation


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test_windowed_agg").getOrCreate()


def make_trips(spark):
    return spark.createDataFrame(
        [
            (date(2023, 1, 1), 8, "Manhattan", 3.5, 12.0, 25.0, 2.0, 15.0),
            (date(2023, 1, 1), 8, "Manhattan", 2.0, 8.0, 20.0, 1.5, 12.0),
            (date(2023, 1, 1), 9, "Queens", 5.0, 20.0, 30.0, 3.0, 18.0),
        ],
        [
            "pickup_date",
            "pickup_hour",
            "pickup_borough",
            "trip_distance",
            "fare_amount",
            "trip_duration_minutes",
            "tip_amount",
            "total_amount",
        ],
    )


def test_hourly_aggregation_groups_correctly(spark):
    df = make_trips(spark)
    result = hourly_aggregation(df)
    manhattan_8 = result.filter(
        (result["pickup_borough"] == "Manhattan") & (result["pickup_hour"] == 8)
    ).first()
    assert manhattan_8["trip_count"] == 2
    assert abs(manhattan_8["total_fare"] - 20.0) < 0.01


def test_daily_aggregation_sums_revenue(spark):
    df = make_trips(spark)
    result = daily_aggregation(df)
    manhattan_day = result.filter(result["pickup_borough"] == "Manhattan").first()
    assert abs(manhattan_day["revenue"] - 27.0) < 0.01


def test_hourly_aggregation_row_count(spark):
    df = make_trips(spark)
    result = hourly_aggregation(df)
    assert result.count() == 2
