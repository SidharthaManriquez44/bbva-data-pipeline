INSERT INTO mart.bank_digital_year (
    bank_code,
    year,
    digital_clients,
    total_clients,
    digital_penetration_pct
)
SELECT
    f.bank_key,
    d.year,
    SUM(f.digital_clients) AS digital_clients,
    SUM(f.total_clients) AS total_clients,
    ROUND(
        (
            SUM(f.digital_clients)::numeric
            / NULLIF(SUM(f.total_clients), 0)
        ) * 100,
        2
    ) AS digital_penetration_pct
FROM core.fact_bank_metrics AS f
INNER JOIN core.dim_date AS d
    ON f.date_key = d.date_key
GROUP BY f.bank_key, d.year;
