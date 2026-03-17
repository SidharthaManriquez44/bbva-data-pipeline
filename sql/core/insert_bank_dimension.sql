BEGIN;

-- 1. Close current record (only if changed)
UPDATE core.dim_bank t
SET
    effective_to = CURRENT_DATE,
    is_current = FALSE
FROM staging.bank_year_metrics_clean AS s
WHERE
    t.bank_code = s.bank_code
    AND t.is_current = TRUE
    AND t.bank_name
    <> CASE
        WHEN s.bank_code = 'BBVA' THEN 'BBVA México'
        ELSE s.bank_code
    END;

-- 2. Insert new version (only real changes)
INSERT INTO core.dim_bank (
    bank_code,
    bank_name,
    effective_from,
    is_current
)
SELECT
    s.bank_code,
    CASE
        WHEN s.bank_code = 'BBVA' THEN 'BBVA México'
        ELSE s.bank_code
    END AS bank_name,
    CURRENT_DATE AS effective_from,
    TRUE AS is_current
FROM staging.bank_year_metrics_clean AS s
LEFT JOIN core.dim_bank AS t
    ON
        s.bank_code = t.bank_code
        AND t.is_current = TRUE
WHERE
    t.bank_code IS NULL
    OR t.bank_name
    <> CASE
        WHEN s.bank_code = 'BBVA' THEN 'BBVA México'
        ELSE s.bank_code
    END;

COMMIT;
