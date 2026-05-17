import os
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import streamlit as st
from botocore.client import Config
from observability.logger import get_logger

logger = get_logger(__name__)

st.set_page_config(page_title="NYC TLC Trip Analytics", layout="wide")

CURATED_ZONE_BUCKET = os.environ.get("MINIO_BUCKET", "nexlab-lake")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")


@st.cache_resource
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def read_parquet_from_s3(prefix: str) -> pd.DataFrame:
    s3 = get_s3()
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=CURATED_ZONE_BUCKET, Prefix=prefix)

    frames = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            response = s3.get_object(Bucket=CURATED_ZONE_BUCKET, Key=key)
            table = pq.read_table(pa.BufferReader(response["Body"].read()))
            frames.append(table.to_pandas())

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


@st.cache_data(ttl=300)
def load_daily_stats() -> pd.DataFrame:
    return read_parquet_from_s3("curated/gold/daily_stats/")


@st.cache_data(ttl=300)
def load_hourly_stats() -> pd.DataFrame:
    return read_parquet_from_s3("curated/gold/hourly_stats/")


@st.cache_data(ttl=300)
def load_fact_trips() -> pd.DataFrame:
    return read_parquet_from_s3("curated/gold/fact_trips/")


def render_q1_revenue_over_time(daily: pd.DataFrame):
    st.subheader("Q1: How does daily revenue trend over time by borough?")

    boroughs = daily["pickup_borough"].dropna().unique().tolist()
    selected = st.multiselect("Select boroughs", boroughs, default=boroughs[:3])

    filtered = daily[daily["pickup_borough"].isin(selected)].copy()
    filtered["pickup_date"] = pd.to_datetime(filtered["pickup_date"])
    pivot = filtered.pivot_table(
        index="pickup_date", columns="pickup_borough", values="revenue", aggfunc="sum"
    )

    st.line_chart(pivot)
    logger.info("q1_rendered", boroughs=selected)


def render_q2_peak_hours(hourly: pd.DataFrame):
    st.subheader("Q2: Which hours have the most trips, and how does it vary by borough?")

    boroughs = hourly["pickup_borough"].dropna().unique().tolist()
    selected_borough = st.selectbox("Select borough", boroughs)

    filtered = hourly[hourly["pickup_borough"] == selected_borough]
    agg = filtered.groupby("pickup_hour")["trip_count"].sum().reset_index()
    agg = agg.sort_values("pickup_hour")

    st.bar_chart(agg.set_index("pickup_hour")["trip_count"])
    logger.info("q2_rendered", borough=selected_borough)


def render_q3_avg_fare_by_distance(fact: pd.DataFrame):
    st.subheader("Q3: What is the relationship between trip distance and average fare?")

    if fact.empty:
        st.warning("No data loaded.")
        return

    sample = fact[["trip_distance", "fare_amount", "pickup_borough"]].dropna()
    sample = sample[(sample["trip_distance"] > 0) & (sample["trip_distance"] < 50)]
    sample["distance_bucket"] = pd.cut(sample["trip_distance"], bins=10)
    agg = sample.groupby("distance_bucket", observed=True)["fare_amount"].mean().reset_index()
    agg["distance_bucket"] = agg["distance_bucket"].astype(str)

    st.bar_chart(agg.set_index("distance_bucket")["fare_amount"])
    logger.info("q3_rendered")


def main():
    st.title("NYC TLC Trip Analytics")
    st.caption("Data platform: PySpark + MinIO + Airflow | Nexlab DE Internship")

    tab1, tab2, tab3 = st.tabs(["Revenue Over Time", "Peak Hours", "Fare vs Distance"])

    with st.spinner("Loading data..."):
        daily = load_daily_stats()
        hourly = load_hourly_stats()
        fact = load_fact_trips()

    with tab1:
        if not daily.empty:
            render_q1_revenue_over_time(daily)
        else:
            st.info("Run the pipeline first to generate data.")

    with tab2:
        if not hourly.empty:
            render_q2_peak_hours(hourly)
        else:
            st.info("Run the pipeline first to generate data.")

    with tab3:
        if not fact.empty:
            render_q3_avg_fare_by_distance(fact)
        else:
            st.info("Run the pipeline first to generate data.")


if __name__ == "__main__":
    main()
