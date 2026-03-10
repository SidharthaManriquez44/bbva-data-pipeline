from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_engine():
    hook = PostgresHook(postgres_conn_id="banking_dw")
    return hook.get_sqlalchemy_engine()
