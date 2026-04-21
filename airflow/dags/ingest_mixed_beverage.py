from datetime import datetime
import _bootstrap  # noqa: F401  (adds repo root to sys.path)
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline.bronze.mixed_beverage import run as ingest_mb

with DAG(
    dag_id="ingest_mixed_beverage",
    description="Monthly ingest of TX mixed beverage receipts (Houston)",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "mixed_beverage"],
) as dag:
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=ingest_mb)
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="build_silver_layer",
        wait_for_completion=False,
    )
    ingest >> trigger_silver
