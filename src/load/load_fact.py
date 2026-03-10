from sqlalchemy import text
from src.config.db_config import get_engine
from src.utils.sql_loader import load_sql


class BankMetricsLoader:
    def __init__(self):
        self.engine = get_engine()
        self.sql = text(load_sql("core", "insert_fact_table.sql"))

    def load(self, run_id):
        with self.engine.begin() as conn:
            result = conn.execute(self.sql, {"run_id": run_id})
            return result.rowcount
