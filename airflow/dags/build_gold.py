from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.gold.aggregates import run as build_gold

with DAG(
    dag_id="build_gold_layer",
    description="Materialize gold analytics aggregates from silver",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold"],
) as dag:
    PythonOperator(task_id="build_aggregates", python_callable=build_gold)
