INSERT INTO mart.bank_growth_year (
    bank_code,
    year,
    loans_yoy_pct,
    deposits_yoy_pct,
    net_income_yoy_pct
)
SELECT
    bank_code,
    year,
    ROUND(
        (
            total_loans
            - LAG(total_loans) OVER w
        )
        / NULLIF(LAG(total_loans) OVER w, 0)
        * 100, 2
    ) AS loans_yoy_pct,
    ROUND(
        (
            total_deposits
            - LAG(total_deposits) OVER w
        )
        / NULLIF(LAG(total_deposits) OVER w, 0)
        * 100, 2
    ) AS deposits_yoy_pct,
    ROUND(
        (
            net_income
            - LAG(net_income) OVER w
        )
        / NULLIF(LAG(net_income) OVER w, 0)
        * 100, 2
    ) AS net_income_yoy_pct

FROM mart.bank_financial_year
WINDOW w AS (
    PARTITION BY bank_code
    ORDER BY year
);
