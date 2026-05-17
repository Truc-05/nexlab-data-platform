# NEXLAB TECHNOLOGY  
## Data Engineer Internship — Entrance Project

# NYC TLC End-to-End Data Platform

> **Stack:** PySpark 3.5 · Airflow 2.9 · MinIO · Streamlit  
> **Date:** May 2026

---

# 1. Problem Statement

This project designs and implements a compact, production-grade end-to-end data platform in one week. The platform ingests real-world NYC Yellow Taxi trip records, transforms the raw data through an engineering-grade pipeline built on distributed processing, and exposes curated results to an analytical dashboard.

---

# 1.1 Dataset

| Property | Value |
|---|---|
| Dataset | NYC TLC Yellow Taxi Trip Records |
| Year | 2023 (Q1 — January, February, March) |
| Raw Size | ~4 GB per quarter, ~80 GB per full year |
| Record Count | ~37 million rows across Q1 |
| Tables | `yellow_tripdata` + `taxi_zone_lookup` |
| Timestamp Columns | `tpep_pickup_datetime`, `tpep_dropoff_datetime` |
| Source | AWS S3 — `d37ci6vzurychx.cloudfront.net` |

---

# 1.2 Business Questions Answered

- How does daily revenue trend over time by NYC borough?
- Which hours of the day have the most trips?
- What is the relationship between trip distance and average fare amount?

---

# 2. Architecture

The platform follows a classic **Lakehouse architecture** with 3 zones:

- **Raw**
- **Silver**
- **Gold**

All jobs are orchestrated by Airflow and executed using a local PySpark cluster. Data is stored in MinIO using Parquet + Snappy compression.

---

## Data Flow

```text
NYC TLC S3
    |
    v
batch_ingest.py
    |
    v
MinIO / raw /
    |
    v
cleansing.py
    |
    v
MinIO / silver /
    |
    v
join_tables.py + windowed_agg.py
    |
    v
MinIO / gold /
    |
    v
Airflow DAG
    |
    v
Streamlit Dashboard
```

---

# 2.1 Tech Stack

| Component | Choice | Justification |
|---|---|---|
| Distributed Engine | PySpark 3.5 | Industry-standard distributed processing |
| Object Storage | MinIO | S3-compatible local object storage |
| File Format | Parquet + Snappy | Fast analytics + compression |
| Orchestration | Airflow 2.9 | DAG scheduling + retries + observability |
| Dashboard | Streamlit | Fast Python-native dashboard |
| Data Quality | Custom checks | Lightweight alternative to Great Expectations |
| IaC | Docker Compose | One-command environment setup |

---

# 3. Data Model

## 3.1 Grain Definition

`fact_trips` grain = **one row per completed taxi trip**

Primary Key:

```text
trip_id = SHA256(
    VendorID ||
    tpep_pickup_datetime ||
    PULocationID
)
```

This deterministic key guarantees idempotent overwrites.

---

## 3.2 Star Schema

```text
dim_date             dim_location
   |                      |
   +-----> fact_trips <---+
```

---

## 3.3 fact_trips Columns

| Column | Type | Description |
|---|---|---|
| trip_id | string | Unique surrogate key |
| pickup_date_sk | int | FK → dim_date |
| pickup_location_sk | int | FK → dim_location |
| dropoff_location_sk | int | FK → dim_location |
| VendorID | int | Taxi vendor |
| tpep_pickup_datetime | timestamp | Trip start |
| tpep_dropoff_datetime | timestamp | Trip end |
| passenger_count | int | Passenger count |
| trip_distance | double | Miles traveled |
| trip_duration_minutes | double | Derived duration |
| fare_amount | double | Fare in USD |
| tip_amount | double | Tip in USD |
| total_amount | double | Total charge |
| pickup_hour | int | Hour of pickup |
| year | int | Partition column |
| month | int | Partition column |

---

## 3.4 dim_location Columns

| Column | Type | Description |
|---|---|---|
| location_sk | int | Surrogate key |
| location_id | int | Original TLC zone ID |
| borough | string | NYC borough |
| zone | string | Zone name |
| service_zone | string | Service zone type |

---

## 3.5 dim_date Columns

| Column | Type |
|---|---|
| date_sk | int |
| full_date | date |
| year | int |
| month | int |
| day | int |
| day_of_week | int |
| week_of_year | int |
| is_weekend | boolean |
| quarter | int |

---

# 4. Partition Strategy

| Zone | Path Pattern | Partition Key | Purpose |
|---|---|---|---|
| Raw | `raw/yellow_tripdata/year=Y/month=M/` | year, month | Monthly re-ingestion |
| Silver | `silver/yellow_tripdata/year=Y/month=M/` | year, month | Lineage tracing |
| Gold fact_trips | `gold/fact_trips/year=Y/month=M/` | month | Query pruning |
| Gold daily_stats | `gold/daily_stats/pickup_date=D/` | pickup_date | Fast dashboard access |
| Gold hourly_stats | `gold/hourly_stats/pickup_date=D/` | pickup_date | Fast dashboard access |

---

# 5. Pipeline & Orchestration

## DAG Structure

```text
ingest
   |
   v
cleanse
   |
   v
join_zones
   |
   +--> build_dims
   |
   +--> build_fact
   |
   +--> aggregate
   |
   v
data_quality
```

---

## 5.1 Idempotency Design

- Skip ingestion if object already exists
- Spark writes use `mode='overwrite'`
- Deterministic `trip_id`
- DQ checks block bad downstream data

---

## 5.2 Failure Modes

| Failure | Detection | Behavior |
|---|---|---|
| Duplicate records | Dedup logic | Dropped + logged |
| Null columns | Validation checks | Quarantine bad rows |
| Late-arriving data | Window filter | Logged + filtered |
| Airflow task failure | Retry policy | Auto retry |
| MinIO unreachable | boto3 exception | Retry + recovery |

---

## 5.3 Observability

Metrics collected:

- `job_duration_seconds`
- `records_processed`
- `data_freshness_lag_seconds`
- `dq_null_count`
- `dq_duplicate_count`
- `dq_out_of_range`

Structured JSON logging is enabled for all jobs.

---

# 6. Data Quality

| Check | Column/Table | Threshold | Failure Behavior |
|---|---|---|---|
| not_null | fact_trips.trip_id | 0 nulls | Block pipeline |
| uniqueness | fact_trips.trip_id | 0 duplicates | Block pipeline |
| range | fare_amount | 0–5000 USD | Block pipeline |
| range | trip_distance | 0–200 miles | Block pipeline |
| referential integrity | FK checks | 0 orphans | Warning only |
| row count threshold | fact_trips | >=100k rows | Block pipeline |

---

# 7. Engineering Decisions

| Decision | Choice | Alternative | Reason |
|---|---|---|---|
| Spark Mode | local[*] | Cloud YARN | Faster setup |
| Load Strategy | Full overwrite | Delta MERGE | Simpler logic |
| Serving | Streamlit | Superset | Faster delivery |
| DQ Framework | Custom checks | Great Expectations | Less overhead |
| File Format | Parquet | Delta/Iceberg | Simpler stack |
| Orchestration | Airflow LocalExecutor | Celery/K8s | Enough for scale |

---

# 7.1 Future Improvements

## Storage Layer
- Delta Lake
- ACID transactions
- Time travel

## Transformation Layer
- dbt integration
- SQL lineage
- Documentation

## Cloud Deployment
- GCS + Dataproc
- S3 + EMR
- Terraform infrastructure

## Streaming
- Kafka producer
- Flink consumer
- Near real-time dashboard

## Metadata
- Apache Atlas
- OpenMetadata

---

# 8. Engineering Practices

| Practice | Implementation |
|---|---|
| Source Control | Git + PR workflow |
| Linter | Ruff |
| Formatter | Black |
| Unit Tests | pytest |
| Integration Tests | End-to-end pipeline test |
| CI/CD | GitHub Actions |
| Containerization | Docker Compose |
| Secret Management | `.env` |
| Documentation | README + Design Doc + Runbook |

---

# 9. Running the Platform

## 9.1 Prerequisites

- Docker Desktop >= 4.x
- Python 3.11+
- Git
- Java 11+

---

## 9.2 Quickstart

```bash
git clone https://github.com/Truc-05/nexlab-data-platform.git
cd nexlab-data-platform

cp .env.example .env

docker compose -f infra/docker-compose.yml up -d
```

### Open Services

- Airflow: http://localhost:8080
- Streamlit: http://localhost:8501

---

# 9.3 Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin/admin |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| Spark Master UI | http://localhost:8082 | none |
| Streamlit Dashboard | http://localhost:8501 | none |

---

# Project Summary

This project demonstrates:

- End-to-end batch data engineering
- Distributed processing with PySpark
- Airflow orchestration
- Lakehouse modeling
- Data quality enforcement
- Dockerized local infrastructure
- Dashboard analytics delivery

---
