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
                SUM(f.total_loans),
                SUM(f.total_deposits),
                SUM(f.net_income)
            FROM core.fact_bank_metrics f
            JOIN core.dim_bank b
            ON f.bank_key = b.bank_key
            JOIN core.dim_date d
            ON f.date_key = d.date_key
            GROUP BY b.bank_code, d.year;