from airflow.models import Variable
from pathlib import Path

DATA_PATH = Path(Variable.get("bbva_data_path"))
PIPELINE_NAME = "bbva_data_pipeline"
