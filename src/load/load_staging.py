from sqlalchemy import text
import time
from src.config.database import get_engine
from src.utils.sql_loader import load_sql


def load_staging_data(df, connection=None):
    batch_id = int(time.time())
    df["batch_id"] = batch_id

    query = text(load_sql("staging", "insert_staging_bank_metrics.sql"))

    records = df.to_dict(orient="records")

    if connection is not None:
        connection.execute(query, records)
        return

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(query, records)
