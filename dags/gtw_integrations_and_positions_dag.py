from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from bi.common import defaults
from bi.dag_resources.integrations.main import main

DAG_NAME = "gtw_integrations_and_positions_dag"
default_args = defaults.defaults(datetime(2026, 1, 31))

dag = DAG(
    DAG_NAME,
    description="Generate GTW integrations and positions output",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["integrations", "bigquery"],
)


def run_gtw_integrations_and_positions():
    return main(MANUAL_RUN=False)


run_gtw_integrations_and_positions_task = PythonOperator(
    task_id="run_gtw_integrations_and_positions",
    python_callable=run_gtw_integrations_and_positions,
    dag=dag,
)