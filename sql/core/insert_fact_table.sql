BEGIN;

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
    etl_run_id,
    hash_diff
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
    :run_id AS etl_run_id,
    MD5(
        CONCAT_WS(
            '|',
            s.branches,
            s.atms,
            s.digital_clients,
            s.total_clients,
            s.total_loans,
            s.total_deposits,
            s.net_income
        )
    ) AS hash_diff
FROM staging.bank_year_metrics_clean AS s

INNER JOIN core.dim_bank AS b
    ON
        s.bank_code = b.bank_code
        AND b.is_current = TRUE

INNER JOIN core.dim_date AS d
    ON d.date = MAKE_DATE(s.year, 12, 31)

INNER JOIN core.dim_channel AS c
    ON
        c.channel_code = 'TOTAL'
        AND c.is_current = TRUE

ON CONFLICT (bank_key, date_key, channel_key)
DO UPDATE SET
    branches = excluded.branches,
    atms = excluded.atms,
    digital_clients = excluded.digital_clients,
    total_clients = excluded.total_clients,
    total_loans = excluded.total_loans,
    total_deposits = excluded.total_deposits,
    net_income = excluded.net_income,
    hash_diff = excluded.hash_diff
WHERE
core.fact_bank_metrics.branches IS DISTINCT FROM excluded.branches
OR core.fact_bank_metrics.atms IS DISTINCT FROM excluded.atms
OR core.fact_bank_metrics.digital_clients IS DISTINCT FROM excluded.digital_clients
OR core.fact_bank_metrics.total_clients IS DISTINCT FROM excluded.total_clients
OR core.fact_bank_metrics.total_loans IS DISTINCT FROM excluded.total_loans
OR core.fact_bank_metrics.total_deposits IS DISTINCT FROM excluded.total_deposits
OR core.fact_bank_metrics.net_income IS DISTINCT FROM excluded.net_income
OR core.fact_bank_metrics.hash_diff IS DISTINCT FROM excluded.hash_diff;

COMMIT;
