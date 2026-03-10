from airflow.decorators import dag, task
from datetime import datetime

from src.orchestrator.pipeline_runner import run_pipeline


@dag(
    dag_id="bbva_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def bbva_pipeline():
    @task
    def run_etl():
        run_pipeline()

    run_etl()


dag = bbva_pipeline()
