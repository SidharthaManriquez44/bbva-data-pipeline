import pytest
import pandas as pd
from src.config.database import get_engine
from dotenv import load_dotenv

load_dotenv(".env.test")


@pytest.fixture
def db_transaction():
    engine = get_engine()
    connection = engine.connect()
    transaction = connection.begin()

    yield connection

    transaction.rollback()
    connection.close()


@pytest.fixture
def fake_data():
    fake_df = pd.DataFrame(
        {
            "year": [2020],
            "digital_penetration_pct": [72.0],
            "branches": [1800],
            "atms": [12000],
            "total_clients": [55000000],
            "digital_clients": [40000000],
            "total_loans": [1500000000000],
            "total_deposits": [1550000000000],
            "net_income": [120000000000],
            "profit_per_branch": [66666],
        }
    )
    return fake_df


@pytest.fixture
def raw_df():
    raw_df = pd.DataFrame(
        {
            "bank_code": ["BBVA", "BBVA", "BBVA"],
            "year": ["2020", "2021", 2022],
            "branches": [1745, "1716", 1732],
            "atms": [12950, "13400", 14019],
            "total_clients": [27500000, "28000000", 29500000],
            "digital_clients": [20000000, 21000000, "22000000"],
            "total_loans": [1450000000000, 1550000000000, "1650000000000"],
            "total_deposits": ["1500000000000", 1600000000000, 1680000000000],
            "net_income": [680000000000, 750000000000, "800000000000"],
        }
    )
    return raw_df


@pytest.fixture
def clean_data_df():
    clean_df = pd.DataFrame(
        {
            "bank_code": ["BBVA", "BBVA", "BBVA"],
            "year": [2020, 2021, 2022],
            "branches": [1745, 1716, 1732],
            "atms": [12950, 13400, 14019],
            "total_clients": [27500000, 28000000, 29500000],
            "digital_clients": [20000000, 21000000, 22000000],
            "total_loans": [1450000000000, 1550000000000, 1650000000000],
            "total_deposits": [1500000000000, 1600000000000, 1680000000000],
            "net_income": [680000000000, 750000000000, 800000000000],
        }
    )
    return clean_df


@pytest.fixture
def fake_query():
    query = "INSERT INTO table VALUES (...)"
    return query


@pytest.fixture
def sample_csv(tmp_path):
    data = {
        "bank_code": ["BBVA", "BBVA", "BBVA"],
        "year": [2023, 2024, 2025],
        "branches": [1700, 1616, 1532],
        "atms": [12950, 13400, 14019],
        "total_clients": [27500000, 28000000, 29500000],
        "digital_clients": [21000000, 22000000, 23000000],
        "total_loans": [1550000000000, 1650000000000, 1750000000000],
        "total_deposits": [1600000000000, 1700000000000, 1780000000000],
        "net_income": [780000000000, 550000000000, 900000000000],
    }
    df = pd.DataFrame(data)
    csv_path = tmp_path / "data.csv"
    df.to_csv(csv_path, index=False)
    return csv_path
