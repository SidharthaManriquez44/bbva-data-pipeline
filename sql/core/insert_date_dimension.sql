BEGIN;

INSERT INTO core.dim_date (
    date_key,
    date,
    year,
    quarter,
    month,
    day,
    month_name,
    day_name,
    week_of_year,
    day_of_week,
    is_weekend,
    is_month_end,
    is_quarter_end,
    is_year_end,
    month_start_date,
    quarter_start_date,
    year_start_date
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    EXTRACT(DAY FROM d)::SMALLINT AS day,
    TO_CHAR(d, 'FMMonth') AS month_name,
    TO_CHAR(d, 'FMDay') AS day_name,
    EXTRACT(isoweek FROM d)::SMALLINT AS week_of_year,
    EXTRACT(ISODOW FROM d)::SMALLINT AS day_of_week,
    EXTRACT(ISODOW FROM d) IN (6,7) AS is_weekend,
    (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE = d AS is_month_end,
    (DATE_TRUNC('quarter', d) + INTERVAL '3 month - 1 day')::DATE = d AS is_quarter_end,
    (DATE_TRUNC('year', d) + INTERVAL '1 year - 1 day')::DATE = d AS is_year_end,
    DATE_TRUNC('month', d)::DATE AS month_start_date,
    DATE_TRUNC('quarter', d)::DATE AS quarter_start_date,
    DATE_TRUNC('year', d)::DATE AS year_start_date
FROM
    GENERATE_SERIES(
        '2000-01-01'::DATE,
        '2035-12-31'::DATE,
        INTERVAL '1 day'
    ) AS d
ON CONFLICT (date_key) DO NOTHING;

COMMIT;
