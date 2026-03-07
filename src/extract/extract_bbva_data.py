from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]

def extract_bbva_data(last_year=None):
    file_path = BASE_DIR / "data" / "bbva_bank_metrics.csv"
    df = pd.read_csv(file_path)

    if last_year:
        df = df[df["year"] > last_year]

    return df
