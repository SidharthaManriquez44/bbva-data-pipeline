from sqlalchemy import text
from src.config.db_config import get_engine
from src.utils.sql_loader import load_sql


def load_dim_date(connection = None):

    query = text(load_sql("core", "insert_date_dimension.sql"))

    if connection is not None:
        connection.execute(query)
        return

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(query)
