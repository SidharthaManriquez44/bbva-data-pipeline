INSERT INTO core.fact_bank_metrics (
    bank_key,
    date_key,
    channel_key,
    branches,
    atms,
    digital_clients,
    total_clients,
    total_loans,
    total_deposits,
    net_income
)
SELECT
    b.bank_key,
    d.date_key,
    c.channel_key,
    s.branches,
    s.atms,
    s.digital_clients,
    s.total_clients,
    s.total_loans,
    s.total_deposits,
    s.net_income
FROM staging.bank_year_metrics_clean s
JOIN core.dim_bank b
    ON b.bank_code = s.bank_code
JOIN core.dim_date d
    ON d.date = MAKE_DATE(s.year, 12, 31)
JOIN core.dim_channel c
    ON c.channel_code = 'TOTAL'
ON CONFLICT (bank_key, date_key, channel_key)
DO UPDATE SET
    branches = EXCLUDED.branches,
    atms = EXCLUDED.atms,
    digital_clients = EXCLUDED.digital_clients,
    total_clients = EXCLUDED.total_clients,
    total_loans = EXCLUDED.total_loans,
    total_deposits = EXCLUDED.total_deposits,
    net_income = EXCLUDED.net_income;
