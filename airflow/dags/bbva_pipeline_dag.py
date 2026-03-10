from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

from src.extract.extract_bbva_data import extract_bbva_data
from src.data_quality.bank_quality_checks import run_bank_quality_checks
from src.load.load_raw import load_raw_data
from src.load.load_staging import load_staging_data
from src.load.bank_dimension import load_dim_bank
from src.load.channel_dimension import load_dim_channel
from src.load.date_dimension import load_dim_date
from src.data_access.etl_run_repository import ETLRunRepository
from src.load.load_fact import BankMetricsLoader
from src.load.load_mart import MartLoader


@dag(
    dag_id="bbva_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def bbva_pipeline():
    @task
    def start_run():
        repo = ETLRunRepository()
        return repo.start_run("bbva_data_pipeline")

    @task
    def extract():
        return extract_bbva_data()

    @task
    def quality(df):
        run_bank_quality_checks(df)
        return df

    @task
    def raw(df):
        load_raw_data(df)
        return df

    @task
    def staging(df):
        load_staging_data(df)

    with TaskGroup("dimensions") as dimensions:

        @task
        def bank():
            load_dim_bank()

        @task
        def channel():
            load_dim_channel()

        @task
        def date():
            load_dim_date()

        bank()
        channel()
        date()

    @task
    def fact(run_id):
        loader = BankMetricsLoader()
        return loader.load(run_id)

    @task
    def marts():
        MartLoader().load_all()

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def finish_run(run_id, rows_loaded):
        repo = ETLRunRepository()
        repo.finish_run(run_id, rows_loaded)

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def fail_run(run_id):
        repo = ETLRunRepository()
        repo.fail_run(run_id, "Pipeline failed")

    # DAG FLOW

    run_id = start_run()

    df = extract()
    df = quality(df)
    df = raw(df)

    staging_task = staging(df)

    staging_task >> dimensions

    rows_loaded = fact(run_id)

    marts_task = marts()

    finish = finish_run(run_id, rows_loaded)
    fail = fail_run(run_id)

    dimensions >> rows_loaded
    rows_loaded >> marts_task
    marts_task >> finish
    marts_task >> fail


dag = bbva_pipeline()
