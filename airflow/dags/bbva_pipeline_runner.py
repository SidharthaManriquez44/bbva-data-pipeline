from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

from src.extract.extract_bbva_data import extract_bbva_data
from src.transform.bank_transformer import clean_bank_metrics
from src.data_quality.bank_quality_checks import run_bank_quality_checks
from src.load.load_raw import load_raw_data
from src.load.load_staging import load_staging_data
from src.load.bank_dimension import load_dim_bank
from src.load.channel_dimension import load_dim_channel
from src.load.date_dimension import load_dim_date
from src.load.load_fact import BankMetricsLoader
from src.load.load_mart import MartLoader

from src.data_access.etl_run_repository import ETLRunRepository
from src.data_access.watermark_repository import WatermarkRepository


@dag(
    dag_id="bbva_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def bbva_pipeline():
    # META

    @task
    def start_run():
        repo = ETLRunRepository()
        return repo.start_run("bbva_data_pipeline")

    @task
    def get_watermark():
        repo = WatermarkRepository()
        return repo.get_last_year("bbva_data_pipeline")

    # EXTRACT

    @task
    def extract(last_year):
        return extract_bbva_data(last_year)

    # TRANSFORM

    @task
    def transform(df):
        return clean_bank_metrics(df)

    # QUALITY

    @task
    def quality(df):
        run_bank_quality_checks(df)
        return df

    # RAW

    @task
    def raw(df):
        load_raw_data(df)
        return df

    # STAGING

    @task
    def staging(df):
        load_staging_data(df)
        return df

    # DIMENSIONS

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

    # FACT

    @task
    def fact(run_id):
        loader = BankMetricsLoader()
        return loader.load(run_id)

    # MARTS

    @task
    def marts():
        MartLoader().load_all()

    # WATERMARK UPDATE

    @task
    def update_watermark(df):
        repo = WatermarkRepository()
        new_year = int(df["year"].max())
        repo.update_last_year("bbva_data_pipeline", new_year)

    # META SUCCESS

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def finish_run(run_id, rows_loaded):
        repo = ETLRunRepository()
        repo.finish_run(run_id, rows_loaded)

    # META FAIL

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def fail_run(run_id):
        repo = ETLRunRepository()
        repo.fail_run(run_id, "Pipeline failed")

    # FLOW

    run_id = start_run()

    last_year = get_watermark()

    df = extract(last_year)

    df = transform(df)

    df = quality(df)

    df = raw(df)

    df = staging(df)

    df >> dimensions

    rows_loaded = fact(run_id)

    marts_task = marts()

    update = update_watermark(df)

    finish = finish_run(run_id, rows_loaded)

    fail = fail_run(run_id)

    # DEPENDENCIES

    run_id >> last_year

    dimensions >> rows_loaded

    rows_loaded >> marts_task >> update

    update >> finish

    update >> fail


dag = bbva_pipeline()
