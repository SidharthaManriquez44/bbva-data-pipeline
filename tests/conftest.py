import pytest
import pandas as pd
from src.config.db_config import get_engine

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
    fake_df = pd.DataFrame({
        "year": [2020],
        "digital_penetration_pct": [72.0],
        "branches": [1800],
        "atms": [12000],
        "total_clients": [55000000],
        "digital_clients": [40000000],
        "total_loans": [1500000000000],
        "total_deposits": [1550000000000],
        "net_income": [120000000000],
        "profit_per_branch": [66666]
    })
    return fake_df
