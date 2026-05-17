import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "nexlab",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

YEAR = int(os.environ.get("NYC_TLC_YEAR", 2023))
MONTHS = [int(m) for m in os.environ.get("NYC_TLC_MONTHS", "1,2,3").split(",")]


def task_ingest(**context):
    from ingestion.batch_ingest import run

    run(year=YEAR, months=MONTHS)


def task_cleanse(**context):
    from processing.utils.spark_session import create_spark_session
    from processing.jobs.cleansing import run

    spark = create_spark_session("cleansing")
    for month in MONTHS:
        run(spark, YEAR, month)
    spark.stop()


def task_join(**context):
    from processing.utils.spark_session import create_spark_session
    from processing.jobs.join_tables import run

    spark = create_spark_session("join_tables")
    run(spark, YEAR, MONTHS)
    spark.stop()


def task_aggregate(**context):
    from processing.utils.spark_session import create_spark_session
    from processing.jobs.windowed_agg import run

    spark = create_spark_session("windowed_agg")
    run(spark, YEAR)
    spark.stop()


def task_build_dims(**context):
    from processing.utils.spark_session import create_spark_session
    from modeling.dim_table import run

    spark = create_spark_session("dim_tables")
    run(spark, YEAR)
    spark.stop()


def task_build_fact(**context):
    from processing.utils.spark_session import create_spark_session
    from modeling.fact_table import run

    spark = create_spark_session("fact_tables")
    run(spark, YEAR)
    spark.stop()


def task_data_quality(**context):
    from processing.utils.spark_session import create_spark_session
    from quality.checks.curated_checks import run

    spark = create_spark_session("data_quality")
    for month in MONTHS:
        run(spark, YEAR, month)
    spark.stop()


with DAG(
    dag_id="end_to_end_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nexlab", "nyc-tlc"],
) as dag:

    ingest = PythonOperator(task_id="ingest", python_callable=task_ingest)
    cleanse = PythonOperator(task_id="cleanse", python_callable=task_cleanse)
    join = PythonOperator(task_id="join_zones", python_callable=task_join)
    aggregate = PythonOperator(task_id="aggregate", python_callable=task_aggregate)
    build_dims = PythonOperator(task_id="build_dims", python_callable=task_build_dims)
    build_fact = PythonOperator(task_id="build_fact", python_callable=task_build_fact)
    dq = PythonOperator(task_id="data_quality", python_callable=task_data_quality)

    ingest >> cleanse >> join >> [aggregate, build_dims] >> build_fact >> dq
