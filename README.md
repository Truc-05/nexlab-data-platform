# NYC TLC Data Platform

End-to-end data engineering platform built on NYC Yellow Taxi trip records (2023 Q1, ~37 million rows). Ingests raw data from a public S3 source, processes it through a distributed PySpark pipeline, models it into a star schema, and serves analytical insights via a Streamlit dashboard — all orchestrated by Airflow and running locally via Docker Compose.

---

## Stack

| Layer | Tool |
|---|---|
| Distributed Processing | PySpark 3.5 |
| Object Storage | MinIO (S3-compatible) |
| File Format | Parquet + Snappy |
| Orchestration | Airflow 2.9 |
| Data Quality | Custom checks (7 checks) |
| Dashboard | Streamlit |
| CI/CD | GitHub Actions |
| IaC | Docker Compose |

---

## Architecture

```
NYC TLC S3 (public)
      |
      v
batch_ingest.py  -->  MinIO / raw / yellow_tripdata / year=2023 / month=01..03
                            |
                     PySpark cleansing.py  (cast, dedup, business rules, derived cols)
                            |
                     MinIO / silver / yellow_tripdata
                            |
              +-------------+-------------+
              |                           |
     join_tables.py               windowed_agg.py
     (trips + taxi_zones)         (hourly, daily, rolling 7d)
              |                           |
              +-------------+-------------+
                            |
                  MinIO / gold /
                    fact_trips        (star schema fact table)
                    dim_location      (265 NYC taxi zones)
                    dim_date          (calendar dimension)
                    daily_stats       (aggregated gold layer)
                    hourly_stats      (aggregated gold layer)
                            |
                     Streamlit Dashboard
                     localhost:8501
```

Airflow DAG `end_to_end_pipeline` orchestrates every step with retry policy (2 retries, 5-minute delay) and idempotent overwrites.

---

## Quickstart

### Prerequisites

- Docker Desktop >= 4.x
- Python 3.11+
- Git
- Java 11+ (only needed for running tests locally)

### 1. Clone

```bash
git clone https://github.com/Truc-05/nexlab-data-platform.git

cd nexlab-data-internship
```

### 2. Configure environment

```bash
cp .env.example .env
```

No changes needed — defaults work out of the box for local Docker setup.

### 3. Start the full stack

```bash
docker compose -f infra/docker-compose.yml up -d
```

This starts 7 services: Postgres, MinIO, MinIO-init, Spark master, Spark worker, Airflow webserver, Airflow scheduler, and the Streamlit dashboard. Wait about 60–90 seconds for all services to become healthy.

```bash
docker compose -f infra/docker-compose.yml ps
```

All services should show `healthy` or `running`.

### 4. Trigger the pipeline

Open Airflow at **http://localhost:8080** and log in with `admin` / `admin`.

- Find the DAG `end_to_end_pipeline`
- Toggle it on if paused
- Click the **Trigger DAG** button (triangle icon)

The pipeline runs in this order:

```
ingest → cleanse → join_zones → [aggregate, build_dims] → build_fact → data_quality
```

The `ingest` task downloads ~4 GB of Parquet files from the NYC TLC public S3 bucket. This takes 5–20 minutes depending on network speed. Subsequent tasks run faster.

### 5. Open the dashboard

Once the pipeline finishes, open **http://localhost:8501** to view the Streamlit dashboard. Three tabs answer three business questions:

- **Revenue Over Time** — daily revenue by NYC borough (time series with drill-down)
- **Peak Hours** — trip count by hour of day per borough
- **Fare vs Distance** — average fare bucketed by trip distance

---

## Project Structure

```
nexlab-de-internship/
├── .github/workflows/ci.yml        # GitHub Actions — lint + test + docker build
├── infra/
│   ├── docker-compose.yml          # Full 7-service local stack
│   ├── Dockerfile.dashboard        # Streamlit container
│   └── terraform/main.tf           # Optional cloud deployment (GCS)
├── ingestion/
│   ├── batch_ingest.py             # Download NYC TLC → MinIO raw zone
│   └── failure_handlers.py         # Dedup, schema quarantine, late-data filter
├── processing/
│   ├── jobs/cleansing.py           # Cast types, business rules, derived columns
│   ├── jobs/join_tables.py         # Join trips with taxi zone dimension
│   ├── jobs/windowed_agg.py        # Hourly agg, daily agg, rolling 7-day revenue
│   └── utils/spark_session.py      # SparkSession factory with MinIO S3A config
├── modeling/
│   ├── fact_table.py               # Build fact_trips (star schema)
│   └── dim_table.py                # Build dim_location + dim_date
├── orchestration/dags/
│   └── end_to_end_pipeline.py      # Airflow DAG — full pipeline
├── quality/checks/
│   └── curated_checks.py           # 7 DQ checks — blocks downstream on failure
├── observability/
│   ├── logger.py                   # Structured JSON logging (no bare print)
│   └── metrics.py                  # job_duration, records_processed, data_freshness
├── serving/dashboard/
│   └── app.py                      # Streamlit — 3 business questions
├── tests/
│   ├── unit/                       # 5 unit test files
│   └── integration/                # 1 end-to-end integration test
├── docs/
│   ├── design_doc.md               # 2–4 page design document
│   ├── architecture.png            # Architecture diagram
│   └── runbook.md                  # 5 failure modes + recovery steps
├── storage/schema/data_dictionary.md
├── .env.example
├── pyproject.toml                  # ruff + black + pytest config
├── Makefile
└── requirements.txt
```

---

## Service URLs

| Service | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8082 | — |
| Streamlit Dashboard | http://localhost:8501 | — |

---

## Running Tests

Install dependencies:

```bash
pip install -r requirements.txt
```

Run all tests:

```bash
make test
```

Run unit tests only:

```bash
make test-unit
```

Run integration test only:

```bash
make test-integration
```

---

## Linting & Formatting

```bash
make lint        # ruff + black --check
make format      # black + ruff --fix
```

---

## Stopping the Stack

```bash
docker compose -f infra/docker-compose.yml down -v
```

The `-v` flag removes volumes. Omit it to keep MinIO data between restarts.

---

## Data Model

Star schema with one fact table and two dimensions.

**fact_trips** — grain: one row per completed trip. Primary key: `trip_id` (SHA-256 of VendorID + pickup_datetime + PULocationID).

**dim_location** — 265 NYC taxi zones with borough and service zone.

**dim_date** — one row per calendar day with weekday, quarter, and weekend flags.

Full column definitions: [storage/schema/data_dictionary.md](storage/schema/data_dictionary.md)

---

## Data Quality Checks

Seven checks run after the fact table is built. Any failure blocks the pipeline:

| Check | Column | Threshold |
|---|---|---|
| not_null | trip_id | 0 nulls |
| not_null | tpep_pickup_datetime | 0 nulls |
| uniqueness | trip_id | 0 duplicates |
| range | fare_amount | 0–5,000 USD |
| range | trip_distance | 0–200 miles |
| referential integrity | pickup_location_sk | 0 orphans (soft warn) |
| row count | fact_trips per month | >= 100,000 rows |

---

## Dataset

- **Source**: NYC TLC Yellow Taxi Trip Records — [https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Year**: 2023 Q1 (January, February, March)
- **Size**: ~4 GB compressed Parquet, ~37 million records
- **Tables**: yellow_tripdata (trips) + taxi_zone_lookup (dimension)
- **License**: Public domain