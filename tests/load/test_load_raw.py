from unittest.mock import patch, MagicMock
from src.load.load_raw import load_raw_data


@patch("src.load.load_raw.load_sql")
@patch("src.load.load_raw.get_engine")
def test_load_raw_data(mock_get_engine, mock_load_sql, fake_data):

    mock_load_sql.return_value = "INSERT INTO table VALUES (...)"

    fake_conn = MagicMock()
    fake_engine = MagicMock()

    mock_get_engine.return_value = fake_engine
    fake_engine.begin.return_value.__enter__.return_value = fake_conn

    load_raw_data(fake_data)

    # engine created
    mock_get_engine.assert_called_once()

    # SQL loaded
    mock_load_sql.assert_called_once()

    # execute called
    fake_conn.execute.assert_called_once()

    args, kwargs = fake_conn.execute.call_args

    records = args[1]

    assert isinstance(records, list)
    assert len(records) == len(fake_data)
    assert "batch_id" in records[0]