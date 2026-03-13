import pytest
from pathlib import Path
import pandas as pd
from src.extract.extract_bbva_data import extract_data


@pytest.mark.parametrize(
    "suffix, writer",
    [
        (".csv", lambda df, path: df.to_csv(path, index=False)),
        (".json", lambda df, path: df.to_json(path, orient="records")),
        (".parquet", lambda df, path: df.to_parquet(path)),
    ],
)
def test_extract_supported_formats(tmp_path, suffix, writer):
    df_expected = pd.DataFrame({"bank_code": ["BBVA"], "year": [2024]})

    path = tmp_path / f"data{suffix}"

    writer(df_expected, path)

    df = extract_data(path)

    assert len(df) == 1
    assert "bank_code" in df.columns


def test_extract_csv(tmp_path):
    df_expected = pd.DataFrame({"bank_code": ["BBVA"], "year": [2024]})

    path = tmp_path / "bbva.csv"
    df_expected.to_csv(path, index=False)

    df = extract_data(path)

    assert len(df) == 1


def test_validate_data(sample_csv):
    df = extract_data(sample_csv)

    assert len(df) == 3


def test_extract_file_not_found():
    with pytest.raises(FileNotFoundError):
        extract_data(Path("fake_file.csv"))


def test_extract_invalid_format(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("invalid")

    with pytest.raises(ValueError):
        extract_data(path)


def test_extract_filter_year(tmp_path):
    df = pd.DataFrame({"bank_code": ["BBVA", "BBVA", "BBVA"], "year": [2020, 2022, 2024]})

    path = tmp_path / "bbva.csv"
    df.to_csv(path, index=False)

    result = extract_data(path, last_year=2021)

    assert len(result) == 2
