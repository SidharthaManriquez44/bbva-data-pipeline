from sqlalchemy import text
from src.config.db_config import get_engine
from src.utils.sql_loader import load_sql


class ETLRunRepository:
    def __init__(self):
        self.engine = get_engine()
        self.insert_run = text(load_sql("meta", "insert_meta_etl_runs.sql"))
        self.update_success = text(load_sql("meta", "update_meta_etl_success.sql"))
        self.update_fail = text(load_sql("meta", "update_meta_etl_fail.sql"))
        self.select_run_last_success = text(load_sql("meta", "select_meta_etl_last_success.sql"))

    def start_run(self, pipeline_name):
        with self.engine.begin() as conn:
            result = conn.execute(self.insert_run, {"pipeline_name": pipeline_name})
            run_id = result.scalar()

        return run_id

    def finish_run(self, run_id, rows_loaded):
        with self.engine.begin() as conn:
            conn.execute(self.update_success, {"run_id": run_id, "rows_loaded": rows_loaded})

    def fail_run(self, run_id, error_message):
        with self.engine.begin() as conn:
            conn.execute(self.update_fail, {"run_id": run_id, "error_message": error_message})

    def get_last_successful_run(self, pipeline_name: str):
        with self.engine.connect() as conn:
            result = conn.execute(self.select_run_last_success, {"pipeline_name": pipeline_name})
            return result.scalar()
