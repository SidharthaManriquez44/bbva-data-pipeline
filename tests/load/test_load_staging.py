from unittest.mock import patch, MagicMock
from src.load.load_staging import load_staging_data


# Test 1 — offline (creates engine)
@patch("src.load.load_staging.load_sql")
@patch("src.load.load_staging.get_engine")
def test_load_staging_data_create_engine(mock_get_engine, mock_load_sql, clean_data_df):
    mock_load_sql.return_value = "SELECT 1"

    fake_conn = MagicMock()
    fake_engine = MagicMock()

    mock_get_engine.return_value = fake_engine
    fake_engine.begin.return_value.__enter__.return_value = fake_conn

    load_staging_data(clean_data_df)

    mock_get_engine.assert_called_once()
    mock_load_sql.assert_called_once()
    fake_conn.execute.assert_called_once()

    # Verify send data
    args, kwargs = fake_conn.execute.call_args
    records = args[1]

    assert isinstance(records, list)
    assert "batch_id" in records[0]


# Test 2: Using an external connection
@patch("src.load.load_staging.load_sql")
def test_load_staging_data_with_connection(mock_load_sql, clean_data_df):
    mock_load_sql.return_value = "SELECT 1"

    fake_conn = MagicMock()

    load_staging_data(clean_data_df, fake_conn)

    mock_load_sql.assert_called_once()
    fake_conn.execute.assert_called_once()

    # Verify send data
    args, kwargs = fake_conn.execute.call_args
    records = args[1]

    assert "batch_id" in records[0]


def test_load_staging_adds_batch_id(clean_data_df):
    fake_conn = MagicMock()

    load_staging_data(clean_data_df, fake_conn)

    assert "batch_id" in clean_data_df.columns
    assert clean_data_df["batch_id"].notnull().all()


@patch("src.load.load_staging.load_sql")
def test_records_sent_to_db(mock_load_sql, clean_data_df):
    mock_load_sql.return_value = "SELECT 1"

    fake_conn = MagicMock()

    load_staging_data(clean_data_df, fake_conn)

    args, kwargs = fake_conn.execute.call_args
    records = args[1]

    assert isinstance(records, list)
    assert len(records) > 0


@patch("src.load.load_staging.load_sql")
def test_sql_file_called(mock_load_sql, clean_data_df):
    mock_load_sql.return_value = "SELECT 1"

    fake_conn = MagicMock()

    load_staging_data(clean_data_df, fake_conn)

    mock_load_sql.assert_called_with("staging", "insert_staging_bank_metrics.sql")
