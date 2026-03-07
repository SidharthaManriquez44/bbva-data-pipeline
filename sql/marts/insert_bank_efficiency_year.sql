 INSERT INTO mart.bank_efficiency_year (
                                         bank_code,
                                         year,
                                         branches,
                                         atms,
                                         clients_per_branch,
                                         loans_per_branch,
                                         deposits_per_branch,
                                         profit_per_branch)
                          SELECT b.bank_code,
                                 d.year,

                                 MAX(f.branches) AS branches,
                                 MAX(f.atms)     AS atms,

                                 ROUND(SUM(f.total_clients)::numeric
                                      / NULLIF(MAX(f.branches),0), 2),

                                 ROUND(SUM(f.total_loans)
                                           / NULLIF(MAX(f.branches), 0), 2),

                                 ROUND(SUM(f.total_deposits)
                                           / NULLIF(MAX(f.branches), 0), 2),

                                 ROUND(SUM(f.net_income)
                                           / NULLIF(MAX(f.branches), 0), 2)

                          FROM core.fact_bank_metrics f
                                   JOIN core.dim_bank b
                                        ON f.bank_key = b.bank_key
                                   JOIN core.dim_date d
                                        ON f.date_key = d.date_key

                          GROUP BY b.bank_code, d.year;
