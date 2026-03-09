from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from src.extract.extract_bbva_data import extract_bbva_data
from src.data_quality.bank_quality_checks import run_bank_quality_checks
from src.load.load_raw import load_raw_data
from src.load.load_staging import load_staging_data
from src.load.bank_dimension import load_dim_bank
from src.load.channel_dimension import load_dim_channel
from src.load.date_dimension import load_dim_date
from src.load.load_fact import load_fact_table
from src.load.load_mart import MartLoader


@dag(dag_id="bbva_data_pipeline", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False)
def bbva_pipeline():
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
    def fact():
        load_fact_table()

    @task
    def marts():
        MartLoader().load_all()

    df = extract()

    df = quality(df)

    df = raw(df)

    staging_task = staging(df)

    staging_task >> dimensions >> fact() >> marts()


dag = bbva_pipeline()
