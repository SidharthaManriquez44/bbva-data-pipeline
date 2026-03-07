INSERT INTO core.dim_date (
    date_key,
    date,
    year,
    quarter,
    month,
    is_year_end
)
SELECT
    (y.year * 10000) + 1231,
    MAKE_DATE(y.year, 12, 31),
    y.year,
    4,
    12,
    TRUE
FROM (
    SELECT DISTINCT year
    FROM staging.bank_year_metrics_clean
) y
LEFT JOIN core.dim_date d
    ON d.date_key = (y.year * 10000) + 1231
WHERE d.date_key IS NULL;
