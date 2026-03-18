from sqlalchemy import text
from src.config.database import get_engine
from src.utils.sql_loader import load_sql


class WatermarkRepository:
    def __init__(self):
        self.engine = get_engine()
        self.select_watermarks = text(load_sql("meta", "select_meta_etl_watermarks.sql"))
        self.update_watermarks = text(load_sql("meta", "update_meta_etl_watermarks.sql"))

    def get_last_year(self, pipeline_name):
        with self.engine.connect() as conn:
            result = conn.execute(self.select_watermarks, {"pipeline_name": pipeline_name})

            return result.scalar()

    def update_last_year(self, pipeline_name, last_year):
        last_year = int(last_year)
        if last_year is None:
            return

        with self.engine.begin() as conn:
            conn.execute(
                self.update_watermarks, {"pipeline_name": pipeline_name, "last_year": last_year}
            )
