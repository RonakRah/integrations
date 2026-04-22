from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import sys

from airflow.decorators import dag, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import main

@dag(
    dag_id="gtw_integrations_and_positions",
    schedule="0 10 * * *",
    start_date=datetime(2026, 4, 22, tzinfo=ZoneInfo("Europe/Prague")),
    catchup=False,
    tags=["integrations", "bigquery"],
)
def gtw_integrations_and_positions_dag():
    @task
    def run_main():
        return main()

    run_main()


gtw_integrations_and_positions = gtw_integrations_and_positions_dag()
