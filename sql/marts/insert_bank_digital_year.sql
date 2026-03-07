 INSERT INTO mart.bank_digital_year (
                                  bank_code,
                                  year,
                                  digital_clients,
                                  total_clients,
                                  digital_penetration_pct)
                          SELECT
                                 f.bank_key,
                                 d.year,
                                 SUM(f.digital_clients),
                                 SUM(f.total_clients),
                                 ROUND(
                                         (SUM(f.digital_clients)::numeric
                     / NULLIF(SUM(f.total_clients),0)) * 100,
                                         2
                                 )
                          FROM core.fact_bank_metrics f
                                   JOIN core.dim_date d
                                        ON f.date_key = d.date_key
                          GROUP BY f.bank_key, d.year;