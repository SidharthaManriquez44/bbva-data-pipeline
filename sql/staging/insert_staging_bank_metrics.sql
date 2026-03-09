WITH ranked_raw AS (
    SELECT
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
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY bank_code, year
            ORDER BY ingestion_timestamp DESC
        ) AS rn
    FROM raw.bank_year_metrics_raw
    WHERE batch_id = (
        SELECT MAX(batch_id)
        FROM raw.bank_year_metrics_raw
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
    CAST(year AS SMALLINT),
    CAST(branches AS INT),
    CAST(atms AS INT),
    CAST(total_clients AS INT),
    CAST(digital_clients AS INT),
    CAST(total_loans AS BIGINT),
    CAST(total_deposits AS BIGINT),
    CAST(net_income AS BIGINT),
    ingestion_timestamp
FROM ranked_raw
WHERE rn = 1
ON CONFLICT (bank_code, year)
DO NOTHING;
