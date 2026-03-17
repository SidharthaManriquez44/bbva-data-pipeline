BEGIN;

WITH source_clean AS (
    SELECT DISTINCT
        bank_code,
        CASE
            WHEN bank_code = 'BBVA' THEN 'BBVA México'
            ELSE bank_code
        END AS bank_name
    FROM staging.bank_year_metrics_clean
),

changes AS (
    SELECT
        s.bank_code,
        s.bank_name,
        t.bank_key,
        t.bank_name AS current_name
    FROM source_clean AS s
    LEFT JOIN core.dim_bank AS t
        ON
            s.bank_code = t.bank_code
            AND t.is_current = TRUE
)

UPDATE core.dim_bank t
SET
    effective_to = CURRENT_DATE,
    is_current = FALSE
FROM changes AS c
WHERE
    t.bank_key = c.bank_key
    AND c.current_name IS NOT NULL
    AND c.current_name <> c.bank_name;

INSERT INTO core.dim_bank (
    bank_code,
    bank_name,
    effective_from,
    is_current
)
SELECT
    c.bank_code,
    c.bank_name,
    CURRENT_DATE AS effective_from,
    TRUE AS is_current
FROM changes AS c
WHERE
    c.current_name IS NULL
    OR c.current_name <> c.bank_name
ON CONFLICT (bank_code, effective_from) DO NOTHING;

COMMIT;
