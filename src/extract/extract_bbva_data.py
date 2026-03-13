from pathlib import Path
import pandas as pd


def extract_data(path: Path, last_year: int | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    readers = {
        ".csv": pd.read_csv,
        ".parquet": pd.read_parquet,
        ".json": pd.read_json,
        ".xlsx": pd.read_excel,
    }

    suffix = path.suffix.lower()

    if suffix not in readers:
        raise ValueError(f"Unsupported file format: {suffix}")

    df = readers[suffix](path)

    if last_year is not None and "year" in df.columns:
        df = df.query("year > @last_year")

    return df
