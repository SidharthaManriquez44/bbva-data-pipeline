def run_bank_quality_checks(df):
    errors = []

    #  null checks
    if df["bank_code"].isnull().any():
        errors.append("bank_code contains NULL values")

    if df["year"].isnull().any():
        errors.append("year contains NULL values")

    #  range check
    if (df["year"] < 2000).any():
        errors.append("year contains invalid values")

    #  negative values
    if (df["total_clients"] < 0).any():
        errors.append("total_clients contains negative values")

    if (df["digital_clients"] < 0).any():
        errors.append("digital_clients contains negative values")

    #  logical validation
    if (df["digital_clients"] > df["total_clients"]).any():
        errors.append("digital_clients greater than total_clients")

    # duplicates
    duplicates = df.duplicated(subset=["bank_code", "year"])

    if duplicates.any():
        errors.append("duplicate bank_code + year records found")

    if errors:
        raise ValueError(f"Data Quality Failed: {errors}")

    return True
