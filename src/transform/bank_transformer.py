import pandas as pd


def clean_bank_metrics(df):
    df = df.copy()

    numeric_cols = [
        "year",
        "branches",
        "atms",
        "total_clients",
        "digital_clients",
        "total_loans",
        "total_deposits",
        "net_income",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["bank_code"] = df["bank_code"].str.upper()

    return df
