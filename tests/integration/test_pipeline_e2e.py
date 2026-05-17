import os
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, TimestampType
)

os.environ.setdefault("MINIO_ACCESS_KEY", "test")
os.environ.setdefault("MINIO_SECRET_KEY", "test")
os.environ.setdefault("MINIO_ENDPOINT", "localhost:9000")
os.environ.setdefault("MINIO_BUCKET", "test-bucket")

@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    base = str(tmp_path_factory.mktemp("lake"))
    os.environ["RAW_ZONE"] = f"file://{base}/raw"
    os.environ["CURATED_ZONE"] = f"file://{base}/curated"

    return (
        SparkSession.builder
        .master("local[1]")
        .appName("integration_test")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

def write_sample_raw(spark, raw_zone: str):
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
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
    ])
    rows = [
        (1, datetime(2023, 1, 10, 8, 0), datetime(2023, 1, 10, 8, 30), 2, 3.5, 132, 236, 12.5, 15.0, 1.5, 0.0),
        (2, datetime(2023, 1, 10, 9, 0), datetime(2023, 1, 10, 9, 20), 1, 2.0, 100, 200, 8.0, 10.0, 1.0, 0.0),
        (1, datetime(2023, 1, 10, 8, 0), datetime(2023, 1, 10, 8, 30), 2, 3.5, 132, 236, 12.5, 15.0, 1.5, 0.0),
    ]
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(f"{raw_zone}/yellow_tripdata/year=2023/month=01/data.parquet")

def write_sample_zones(spark, raw_zone: str):
    zones = spark.createDataFrame(
        [(132, "Queens", "JFK Airport", "Airports"), (236, "Manhattan", "Upper East Side N", "Yellow Zone"),
         (100, "Brooklyn", "Downtown Brooklyn", "Boro Zone"), (200, "Bronx", "Hunts Point", "Boro Zone")],
        ["LocationID", "Borough", "Zone", "service_zone"],
    )
    zones.write.mode("overwrite").option("header", "true").csv(f"{raw_zone}/taxi_zones/taxi_zone_lookup.csv")

def test_cleansing_removes_duplicates_and_outputs_silver(spark):
    from processing.jobs.cleansing import run as run_cleanse
    raw_zone = os.environ["RAW_ZONE"]
    curated_zone = os.environ["CURATED_ZONE"]

    write_sample_raw(spark, raw_zone)
    run_cleanse(spark, year=2023, month=1)

    result = spark.read.parquet(f"{curated_zone}/silver/yellow_tripdata")
    assert result.count() == 2

def test_join_enriches_with_borough(spark):
    from processing.jobs.join_tables import run as run_join
    raw_zone = os.environ["RAW_ZONE"]
    curated_zone = os.environ["CURATED_ZONE"]

    write_sample_zones(spark, raw_zone)
    run_join(spark, year=2023, months=[1])

    result = spark.read.parquet(f"{curated_zone}/silver/trips_with_zones")
    row = result.filter(result["PULocationID"] == 132).first()
    assert row["pickup_borough"] == "Queens"

def test_aggregation_produces_gold_layer(spark):
    from processing.jobs.windowed_agg import run as run_agg
    curated_zone = os.environ["CURATED_ZONE"]

    run_agg(spark, year=2023)

    daily = spark.read.parquet(f"{curated_zone}/gold/daily_stats")
    assert daily.count() > 0
    assert "revenue" in daily.columns
