# Runbook — NYC TLC Data Platform

## Failure Mode 1: Ingestion fails partway through a month

Symptom: Airflow task `ingest` fails mid-run. Partial Parquet file in raw zone.

Detection: Airflow shows task in red. `metrics_dump.json` shows `ingestion_size_mb` below expected (~1.2GB per month).

Recovery:
1. Identify the failed month from Airflow logs.
2. Delete the partial object from MinIO:
   ```bash
   mc rm --recursive local/nexlab-lake/raw/yellow_tripdata/year=2023/month=XX/
   ```
3. Re-trigger the DAG from Airflow UI. Ingestion checks `object_exists` before downloading, so re-runs are safe.

---

## Failure Mode 2: Spark job runs out of memory (OOM)

Symptom: Spark executor killed. Task shows `ExecutorLostFailure` or `OutOfMemoryError` in logs.

Detection: Spark UI at http://localhost:8082 shows failed stages.

Recovery:
1. Increase worker memory in `infra/docker-compose.yml`:
   ```yaml
   SPARK_WORKER_MEMORY: 4G
   ```
2. Add shuffle partition config in `processing/utils/spark_session.py`:
   ```python
   .config("spark.sql.shuffle.partitions", "8")
   ```
3. Restart worker: `make down && make up`.
4. Re-trigger the failed DAG task from Airflow (use "Clear" on the failed task).

---

## Failure Mode 3: Data quality check blocks downstream tasks

Symptom: `data_quality` Airflow task raises `DataQualityFailure`. All downstream tasks blocked.

Detection: Airflow task log shows which check failed and the exact count of violations.

Recovery:
1. Read the error message to identify which check failed (e.g. `fact_trips has 0 rows`).
2. Check if upstream tasks (cleansing, join, aggregation) completed successfully in Airflow.
3. If upstream failed silently, clear and re-run from the first failing task.
4. If the check threshold is wrong (e.g. small dataset in testing), adjust `min_rows` in
   `quality/checks/curated_checks.py` and redeploy.
5. Once root cause is fixed, clear the `data_quality` task and re-run.

---

## Failure Mode 4: MinIO not reachable from Spark or dashboard

Symptom: `ConnectionRefused` or `S3AFileSystem` error in Spark logs. Dashboard shows empty charts.

Detection: `docker compose ps` shows minio container is not healthy.

Recovery:
1. Check MinIO health:
   ```bash
   docker compose -f infra/docker-compose.yml logs minio
   ```
2. Restart MinIO:
   ```bash
   docker compose -f infra/docker-compose.yml restart minio
   ```
3. Verify bucket exists:
   ```bash
   mc ls local/nexlab-lake
   ```
   If missing, re-run `minio-init` service:
   ```bash
   docker compose -f infra/docker-compose.yml up minio-init
   ```

---

## Failure Mode 5: Airflow scheduler not picking up new DAG

Symptom: New DAG not visible in Airflow UI after adding or modifying a file in `orchestration/dags/`.

Detection: DAG missing from http://localhost:8080.

Recovery:
1. Check for syntax errors in the DAG file:
   ```bash
   python orchestration/dags/end_to_end_pipeline.py
   ```
2. Check Airflow scheduler logs:
   ```bash
   docker compose -f infra/docker-compose.yml logs airflow-scheduler
   ```
3. Force DAG reload:
   ```bash
   docker compose -f infra/docker-compose.yml restart airflow-scheduler
   ```
