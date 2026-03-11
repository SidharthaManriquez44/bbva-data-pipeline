from pandas.api.types import is_integer_dtype
from src.transform.bank_transformer import clean_bank_metrics


def test_clean_bank_metrics(raw_df):
    df = clean_bank_metrics(raw_df)

    # dataframe exists
    assert not df.empty

    # columns
    assert "bank_code" in df.columns
    assert "year" in df.columns

    # types
    assert is_integer_dtype(df["total_clients"])

    # data quality
    assert df["total_clients"].sum() == 85000000


def test_schema_bank_metrics(raw_df):
    df = clean_bank_metrics(raw_df)

    expected_columns = {
        "bank_code",
        "year",
        "branches",
        "atms",
        "total_clients",
        "digital_clients",
        "total_loans",
        "total_deposits",
        "net_income",
    }

    assert set(df.columns) == expected_columns


def test_dtypes_bank_metrics(raw_df):
    df = clean_bank_metrics(raw_df)

    assert is_integer_dtype(df["year"])
    assert is_integer_dtype(df["total_clients"])
    assert is_integer_dtype(df["total_loans"])


def test_no_nulls(raw_df):
    df = clean_bank_metrics(raw_df)

    assert df["bank_code"].notnull().all()
    assert df["year"].notnull().all()


def test_business_rules(raw_df):
    df = clean_bank_metrics(raw_df)

    assert (df["digital_clients"] <= df["total_clients"]).all()
