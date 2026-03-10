WITH ranked_raw AS (
    SELECT
        r.raw_id,
        r.bank_code,
        r.year,
        r.branches,
        r.atms,
        r.total_clients,
        r.digital_clients,
        r.total_loans,
        r.total_deposits,
        r.net_income,
        r.ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY r.bank_code, r.year
            ORDER BY r.ingestion_timestamp DESC
        ) AS rn
    FROM raw.bank_year_metrics_raw AS r
    WHERE r.batch_id = (
        SELECT MAX(ry.batch_id)
        FROM raw.bank_year_metrics_raw AS ry
    )
)

INSERT INTO staging.bank_year_metrics_clean (
    raw_id,
    bank_code,
    year,
    branches,
    atms,
    total_clients,
    digital_clients,
    total_loans,
    total_deposits,
    net_income,
    ingestion_timestamp
)
SELECT
    raw_id,
    bank_code,
    CAST(year AS SMALLINT) AS year,
    CAST(branches AS INT) AS branches,
    CAST(atms AS INT) AS atmw,
    CAST(total_clients AS INT) AS total_clients,
    CAST(digital_clients AS INT) AS digital_clients,
    CAST(total_loans AS BIGINT) AS total_loans,
    CAST(total_deposits AS BIGINT) AS total_deposits,
    CAST(net_income AS BIGINT) AS net_income,
    ingestion_timestamp
FROM ranked_raw
WHERE rn = 1
ON CONFLICT (bank_code, year)
DO NOTHING;
