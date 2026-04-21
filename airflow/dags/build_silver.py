from datetime import datetime
import _bootstrap  # noqa: F401
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from pipeline.silver.clean_mixed_beverage import run as clean_mb
from pipeline.silver.clean_inspections import run as clean_insp
from pipeline.silver.match_establishments import run as match_est

with DAG(
    dag_id="build_silver_layer",
    description="Clean bronze into silver + fuzzy-match establishments",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver"],
) as dag:
    t_mb = PythonOperator(task_id="clean_mixed_beverage", python_callable=clean_mb)
    t_insp = PythonOperator(task_id="clean_inspections", python_callable=clean_insp)
    t_match = PythonOperator(task_id="match_establishments", python_callable=match_est)
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="build_gold_layer",
        wait_for_completion=False,
    )
    [t_mb, t_insp] >> t_match >> trigger_gold
