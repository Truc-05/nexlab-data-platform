import pytest
from pyspark.sql import SparkSession
from processing.jobs.join_tables import join_zones

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_join")
        .getOrCreate()
    )

def test_join_adds_pickup_borough(spark):
    trips = spark.createDataFrame(
        [(1, 132, 236)],
        ["VendorID", "PULocationID", "DOLocationID"],
    )
    zones = spark.createDataFrame(
        [(132, "Queens", "JFK Airport", "Airports"), (236, "Manhattan", "Upper East Side N", "Yellow Zone")],
        ["location_id", "borough", "zone", "service_zone"],
    )
    result = join_zones(trips, zones)
    row = result.first()
    assert row["pickup_borough"] == "Queens"
    assert row["dropoff_borough"] == "Manhattan"

def test_join_unknown_location_is_null(spark):
    trips = spark.createDataFrame(
        [(1, 999, 236)],
        ["VendorID", "PULocationID", "DOLocationID"],
    )
    zones = spark.createDataFrame(
        [(236, "Manhattan", "Upper East Side N", "Yellow Zone")],
        ["location_id", "borough", "zone", "service_zone"],
    )
    result = join_zones(trips, zones)
    row = result.first()
    assert row["pickup_borough"] is None

def test_join_preserves_trip_count(spark):
    trips = spark.createDataFrame(
        [(1, 132, 236), (2, 132, 236), (3, 132, 236)],
        ["VendorID", "PULocationID", "DOLocationID"],
    )
    zones = spark.createDataFrame(
        [(132, "Queens", "JFK Airport", "Airports"), (236, "Manhattan", "Upper East Side N", "Yellow Zone")],
        ["location_id", "borough", "zone", "service_zone"],
    )
    result = join_zones(trips, zones)
    assert result.count() == 3
