from unittest.mock import patch, MagicMock
from src.load.channel_dimension import load_dim_channel


# Test 1: Using an external connection
@patch("src.load.channel_dimension.load_sql")
def test_load_dim_bank_with_connection(mock_load_sql):
    mock_load_sql.return_value = "SELECT 1"

    fake_conn = MagicMock()

    load_dim_channel(fake_conn)

    mock_load_sql.assert_called_once()

    fake_conn.execute.assert_called_once()


# Test 2 — offline (creates engine)
@patch("src.load.channel_dimension.load_sql")
@patch("src.load.channel_dimension.get_engine")
def test_load_dim_channel_create_engine(mock_get_engine, mock_load_sql):
    mock_load_sql.return_value = "SELECT 1"

    fake_conn = MagicMock()
    fake_engine = MagicMock()

    mock_get_engine.return_value = fake_engine
    fake_engine.begin.return_value.__enter__.return_value = fake_conn

    load_dim_channel()

    mock_get_engine.assert_called_once()

    fake_conn.execute.assert_called_once()
