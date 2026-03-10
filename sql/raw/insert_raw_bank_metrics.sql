INSERT INTO raw.bank_year_metrics_raw (
    batch_id,
    bank_code,
    year,
    branches,
    atms,
    total_clients,
    digital_clients,
    total_loans,
    total_deposits,
    net_income,
    source_file_name
)
VALUES (
    : batch_id,
    : bank_code,
    : year,
    : branches,
    : atms,
    : total_clients,
    : digital_clients,
    : total_loans,
    : total_deposits,
    : net_income,
    : source_file_name
);
