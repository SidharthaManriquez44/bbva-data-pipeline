import os
from airflow.models import Variable
from pathlib import Path

try:
    DATA_PATH = Path(Variable.get("bbva_data_path"))
except Exception:
    DATA_PATH = Path(os.getenv("BBVA_DATA_PATH", "data/bbva_bank_metrics.csv"))

PIPELINE_NAME = "bbva_data_pipeline"
OUTPUT_RAW = Path("/opt/airflow/data/intermediate/raw.parquet")
OUTPUT_STAGING = Path("/opt/airflow/data/intermediate/staging.parquet")
