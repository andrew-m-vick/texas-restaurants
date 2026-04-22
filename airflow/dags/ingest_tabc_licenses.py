from datetime import datetime
import _bootstrap  # noqa: F401
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline.bronze.tabc_licenses import run as ingest_tabc

with DAG(
    dag_id="ingest_tabc_licenses",
    description="Monthly ingest of TABC license/permit records (Retail tier, TARGET_CITIES)",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "licenses"],
) as dag:
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=ingest_tabc)
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="build_silver_layer",
        wait_for_completion=False,
    )
    ingest >> trigger_silver
