from datetime import datetime
import _bootstrap  # noqa: F401
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline.bronze.austin_inspections import run as ingest_austin

with DAG(
    dag_id="ingest_inspections",
    description="Weekly ingest of Austin food establishment inspection scores",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "inspections"],
) as dag:
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=ingest_austin)
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="build_silver_layer",
        wait_for_completion=False,
    )
    ingest >> trigger_silver
