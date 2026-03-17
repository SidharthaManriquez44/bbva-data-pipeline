BEGIN;

WITH source_clean AS (
    SELECT DISTINCT
        bank_code,
        CASE
            WHEN bank_code = 'BBVA' THEN 'BBVA México'
            ELSE bank_code
        END AS bank_name
    FROM staging.bank_year_metrics_clean
)

-- 1. Close current record
UPDATE core.dim_bank t
SET
    effective_to = CURRENT_DATE,
    is_current = FALSE
FROM source_clean AS s
WHERE
    t.bank_code = s.bank_code
    AND t.is_current = TRUE
    AND t.bank_name <> s.bank_name;

-- 2. Insert new version
INSERT INTO core.dim_bank (
    bank_code,
    bank_name,
    effective_from,
    is_current
)
SELECT
    s.bank_code,
    s.bank_name,
    CURRENT_DATE AS effective_from,
    TRUE AS is_current
FROM source_clean AS s
LEFT JOIN core.dim_bank AS t
    ON
        s.bank_code = t.bank_code
        AND t.is_current = TRUE
WHERE
    (
        t.bank_code IS NULL
        OR t.bank_name <> s.bank_name
    )
    AND NOT EXISTS (
        SELECT 1
        FROM core.dim_bank AS t2
        WHERE
            t2.bank_code = s.bank_code
            AND t2.effective_from = CURRENT_DATE
    );

COMMIT;
