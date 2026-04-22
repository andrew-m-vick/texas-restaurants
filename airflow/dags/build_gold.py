from datetime import datetime
import _bootstrap  # noqa: F401
from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.gold.aggregates import run as build_gold
from pipeline.export.static_json import run as export_static_json

with DAG(
    dag_id="build_gold_layer",
    description="Materialize gold analytics aggregates from silver, then export static JSON",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold"],
) as dag:
    t_gold = PythonOperator(task_id="build_aggregates", python_callable=build_gold)
    t_export = PythonOperator(task_id="export_static_json", python_callable=export_static_json)
    t_gold >> t_export
