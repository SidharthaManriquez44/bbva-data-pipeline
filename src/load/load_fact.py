from sqlalchemy import text
from src.config.db_config import get_engine
from src.utils.sql_loader import load_sql

def load_fact_table(connection = None):

    query = text(load_sql("core", "insert_fact_table.sql"))

    if connection:
        connection.execute(query)
    else:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(query)