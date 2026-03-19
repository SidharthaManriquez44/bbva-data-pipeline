import os
from sqlalchemy import create_engine
from src.config.settings import settings


def get_engine():
    """
    Universal database engine.

    - Uses Airflow connection if running inside Airflow
    - Uses environment variables otherwise (CI, tests, scripts)
    """
    # This automatically triggers validation.
    _ = settings
    # Detect Airflow runtime
    if os.getenv("AIRFLOW_CTX_DAG_ID"):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="banking_dw")
        return hook.get_sqlalchemy_engine()

    # Default environment connection
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ["DB_HOST"]
    port = os.environ["DB_PORT"]
    db = os.environ["DB_NAME"]

    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_size=5,
        max_overflow=10,
    )
