from src.data_quality.bank_quality_checks import run_bank_quality_checks
from src.transform.bank_transformer import clean_bank_metrics


def test_validate_bank_quality_checks(raw_df):
    clean_df = clean_bank_metrics(raw_df)
    df = run_bank_quality_checks(clean_df)

    # True, means that the sanity check do not find any error in the data
    assert df is True
