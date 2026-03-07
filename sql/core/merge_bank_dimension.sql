MERGE INTO core.dim_bank AS target
USING (
    SELECT DISTINCT
        bank_code,
        CASE
            WHEN bank_code = 'BBVA' THEN 'BBVA México'
            ELSE bank_code
        END AS bank_name,
        'México' AS country
    FROM staging.bank_year_metrics_clean
) AS source
ON target.bank_code = source.bank_code

WHEN NOT MATCHED THEN
INSERT (
    bank_code,
    bank_name,
    country
)
VALUES (
    source.bank_code,
    source.bank_name,
    source.country
);
