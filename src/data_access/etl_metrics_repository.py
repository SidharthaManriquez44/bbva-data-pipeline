from sqlalchemy import text
from src.config.database import get_engine
from src.utils.sql_loader import load_sql


class ETLMetricsRepository:
    def __init__(self):
        self.engine = get_engine()
        self.insert_meta = text(load_sql("meta", "insert_meta_etl_step_metrics.sql"))

    def insert_metric(self, pipeline_name: str, step_name: str, rows: int):
        with self.engine.begin() as conn:
            result = conn.execute(
                self.insert_meta,
                {
                    "pipeline_name": pipeline_name,
                    "step_name": step_name,
                    "rows": rows,
                },
            )

            return result.scalar()
