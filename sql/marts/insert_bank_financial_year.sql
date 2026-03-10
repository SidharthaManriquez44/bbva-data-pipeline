INSERT INTO mart.bank_financial_year (
    bank_code,
    year,
    total_loans,
    total_deposits,
    net_income
)
SELECT
    b.bank_code,
    d.year,
    SUM(f.total_loans) AS total_loans,
    SUM(f.total_deposits) AS total_deposits,
    SUM(f.net_income) AS net_income
FROM core.fact_bank_metrics AS f
INNER JOIN core.dim_bank AS b
    ON f.bank_key = b.bank_key
INNER JOIN core.dim_date AS d
    ON f.date_key = d.date_key
GROUP BY b.bank_code, d.year;
