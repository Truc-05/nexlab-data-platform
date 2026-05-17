# NYC TLC Data Platform — Nexlab DE Internship

End-to-end data platform built on NYC Yellow Taxi trip records.
Stack: PySpark + Airflow + MinIO + Streamlit.

## Quickstart

1. Clone repo:
   ```bash
   git clone <repo-url> && cd nexlab-de-internship
   ```

2. Copy environment file:
   ```bash
   cp .env.example .env
   ```

3. Start full stack:
   ```bash
   make up
   ```

4. Trigger the pipeline from Airflow UI at http://localhost:8080 (admin / admin), enable and trigger the `end_to_end_pipeline` DAG.

5. Open dashboard at http://localhost:8501.

## Architecture

See [docs/design_doc.md](docs/design_doc.md) and [docs/architecture.png](docs/architecture.png).

## Run tests locally

```bash
pip install pyspark==3.5.0 pyarrow boto3 requests pytest ruff black
make test
```

## Run lint

```bash
make lint
```
