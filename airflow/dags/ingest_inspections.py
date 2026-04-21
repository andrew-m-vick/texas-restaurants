from datetime import datetime
import _bootstrap  # noqa: F401
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline.bronze.austin_inspections import run as ingest_austin
from pipeline.bronze.dallas_inspections import run as ingest_dallas

with DAG(
    dag_id="ingest_inspections",
    description="Ingest Austin (live) + Dallas (historical) inspections and violations",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bronze", "inspections"],
) as dag:
    t_austin = PythonOperator(task_id="austin", python_callable=ingest_austin)
    t_dallas = PythonOperator(task_id="dallas", python_callable=ingest_dallas)
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="build_silver_layer",
        wait_for_completion=False,
    )
    [t_austin, t_dallas] >> trigger_silver
