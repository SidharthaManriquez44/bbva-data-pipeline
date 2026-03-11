from sqlalchemy import text
from src.config.db_config import get_engine
from src.config.logger_config import get_logger
from src.utils.sql_loader import load_sql


class MartLoader:
    def __init__(self):
        self.engine = get_engine()
        self.logger = get_logger(__name__)

    def _run_mart(self, table_name: str, sql_file: str):
        self.logger.info(f"Loading {table_name}...")

        truncate_sql = text(f"TRUNCATE {table_name};")
        insert_sql = text(load_sql("marts", sql_file))

        with self.engine.begin() as conn:
            conn.execute(truncate_sql)
            conn.execute(insert_sql)

        self.logger.info(f"{table_name} loaded successfully.")

    def load_bank_financial_year(self):
        self._run_mart("mart.bank_financial_year", "insert_bank_financial_year.sql")

    def load_bank_digital_year(self):
        self._run_mart("mart.bank_digital_year", "insert_bank_digital_year.sql")

    def load_bank_efficiency_year(self):
        self._run_mart("mart.bank_efficiency_year", "insert_bank_efficiency_year.sql")

    def load_bank_growth_year(self):
        self._run_mart("mart.bank_growth_year", "insert_bank_growth_year.sql")

    def load_all(self):
        # Important order by departments
        self.load_bank_financial_year()
        self.load_bank_digital_year()
        self.load_bank_efficiency_year()
        self.load_bank_growth_year()
