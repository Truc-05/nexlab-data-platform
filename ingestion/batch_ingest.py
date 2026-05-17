import os
import io
import requests
import boto3
from botocore.client import Config
from observability.logger import get_logger
from observability.metrics import record_metric

logger = get_logger(__name__)

NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.environ['MINIO_ENDPOINT']}",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def build_url(year: int, month: int) -> str:
    return f"{NYC_TLC_BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def build_s3_key(year: int, month: int) -> str:
    return f"raw/yellow_tripdata/year={year}/month={month:02d}/data.parquet"


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False


def download_to_memory(url: str) -> bytes:
    response = requests.get(url, timeout=300, stream=True)
    response.raise_for_status()
    buffer = io.BytesIO()
    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
        buffer.write(chunk)
    return buffer.getvalue()


def ingest_month(year: int, month: int):
    client = get_s3_client()
    bucket = os.environ["MINIO_BUCKET"]
    key = build_s3_key(year, month)

    if object_exists(client, bucket, key):
        logger.info("already_ingested", year=year, month=month, key=key)
        return

    url = build_url(year, month)
    logger.info("download_start", url=url)

    data = download_to_memory(url)
    size_mb = len(data) / (1024 * 1024)

    client.put_object(Bucket=bucket, Key=key, Body=data)

    record_metric("ingestion_size_mb", size_mb, year=year, month=month)
    logger.info("upload_done", key=key, size_mb=round(size_mb, 2))


def ingest_taxi_zones():
    client = get_s3_client()
    bucket = os.environ["MINIO_BUCKET"]
    key = "raw/taxi_zones/taxi_zone_lookup.csv"

    if object_exists(client, bucket, key):
        logger.info("taxi_zones_already_ingested")
        return

    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    data = download_to_memory(url)
    client.put_object(Bucket=bucket, Key=key, Body=data)
    logger.info("taxi_zones_uploaded", key=key)


def run(year: int, months: list[int]):
    ingest_taxi_zones()
    for month in months:
        try:
            ingest_month(year, month)
        except Exception as e:
            logger.error("ingest_failed", year=year, month=month, error=str(e))
            raise


if __name__ == "__main__":
    year = int(os.environ.get("NYC_TLC_YEAR", 2023))
    months = [int(m) for m in os.environ.get("NYC_TLC_MONTHS", "1,2,3").split(",")]
    run(year, months)
