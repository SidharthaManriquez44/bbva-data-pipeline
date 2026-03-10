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
    net_income,
    etl_run_id
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
    s.net_income,
    e.etl_run_id
FROM staging.bank_year_metrics_clean AS s
INNER JOIN core.dim_bank AS b
    ON s.bank_code = b.bank_code
INNER JOIN core.dim_date AS d
    ON d.date = MAKE_DATE(s.year, 12, 31)
INNER JOIN core.dim_channel AS c
    ON c.channel_code = 'TOTAL'
INNER JOIN meta.etl_runs AS e
    ON s.etl_run_id = e.run_id
ON CONFLICT (bank_key, date_key, channel_key)
DO UPDATE SET
    branches = excluded.branches,
    atms = excluded.atms,
    digital_clients = excluded.digital_clients,
    total_clients = excluded.total_clients,
    total_loans = excluded.total_loans,
    total_deposits = excluded.total_deposits,
    net_income = excluded.net_income;
