from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import pandas as pd

from src.extract.extract_bbva_data import extract_data
from src.transform.bank_transformer import clean_bank_metrics
from src.data_quality.bank_quality_checks import run_bank_quality_checks
from src.load.load_raw import load_raw_data
from src.load.load_staging import load_staging_data
from src.load.bank_dimension import load_dim_bank
from src.load.channel_dimension import load_dim_channel
from src.load.date_dimension import load_dim_date
from src.load.load_fact import BankMetricsLoader
from src.load.load_mart import MartLoader
from src.config.pipeline_settings import DATA_PATH, PIPELINE_NAME, OUTPUT_RAW, OUTPUT_STAGING

from src.data_access.etl_run_repository import ETLRunRepository
from src.data_access.watermark_repository import WatermarkRepository


@dag(
    dag_id=PIPELINE_NAME,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def bbva_pipeline():
    # META

    @task
    def start_run():
        repo = ETLRunRepository()
        return repo.start_run(PIPELINE_NAME)

    @task
    def get_watermark():
        repo = WatermarkRepository()
        return repo.get_last_year(PIPELINE_NAME)

    # EXTRACT

    @task
    def extract(last_year):
        df = extract_data(DATA_PATH, last_year)

        output = OUTPUT_RAW
        output.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output)

        return str(output)

    # TRANSFORM

    @task
    def transform(input_path):
        df = pd.read_parquet(input_path)

        df = clean_bank_metrics(df)

        output = OUTPUT_STAGING
        output.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(output)

        return str(output)

    # QUALITY

    @task
    def quality(input_path):
        df = pd.read_parquet(input_path)

        run_bank_quality_checks(df)

        return input_path

    # RAW

    @task
    def raw(input_path):
        df = pd.read_parquet(input_path)

        load_raw_data(df)

        return input_path

    # STAGING

    @task
    def staging(input_path):
        df = pd.read_parquet(input_path)

        load_staging_data(df)

        return input_path

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
    def fact(etl_run_id):
        loader = BankMetricsLoader()
        return loader.load(etl_run_id)

    # MARTS

    @task
    def marts():
        MartLoader().load_all()

    # WATERMARK UPDATE

    @task
    def update_watermark(input_path):
        df = pd.read_parquet(input_path)

        repo = WatermarkRepository()

        new_year = int(df["year"].max())

        repo.update_last_year(PIPELINE_NAME, new_year)

    # META SUCCESS

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def finish_run(etl_run_id, rows_loaded):
        repo = ETLRunRepository()
        repo.finish_run(etl_run_id, rows_loaded)

    # META FAIL

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def fail_run(etl_run_id):
        repo = ETLRunRepository()
        repo.fail_run(etl_run_id, "Pipeline failed")

    # FLOW

    etl_run_id = start_run()

    last_year = get_watermark()

    df_extracted = extract(last_year)

    df_transformed = transform(df_extracted)

    df_quality = quality(df_transformed)

    df_raw = raw(df_quality)

    df_staging = staging(df_raw)

    df_staging >> dimensions

    rows_loaded = fact(etl_run_id)

    marts_task = marts()

    update = update_watermark(df_staging)

    finish = finish_run(etl_run_id, rows_loaded)

    fail = fail_run(etl_run_id)

    # DEPENDENCIES

    etl_run_id >> last_year

    dimensions >> rows_loaded

    rows_loaded >> marts_task >> update

    update >> finish

    update >> fail


dag = bbva_pipeline()
