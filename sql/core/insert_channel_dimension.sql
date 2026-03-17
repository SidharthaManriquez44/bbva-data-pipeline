BEGIN;

-- close if changed
UPDATE core.dim_channel
SET
    effective_to = CURRENT_DATE,
    is_current = FALSE
WHERE
    channel_code = 'TOTAL'
    AND is_current = TRUE
    AND (
        channel_name <> 'Total Banco'
        OR channel_description <> 'Total aggregate without segmentation by channel'
    );

-- insert if it does not exist or has changed
INSERT INTO core.dim_channel (
    channel_code,
    channel_name,
    channel_description,
    effective_from,
    is_current
)
SELECT
    'TOTAL' AS channel_code,
    'Total Banco' AS channel_name,
    'Total aggregate without segmentation by channel' AS channel_description,
    CURRENT_DATE AS effective_from,
    TRUE AS is_current
WHERE NOT EXISTS (
    SELECT 1
    FROM core.dim_channel
    WHERE
        channel_code = 'TOTAL'
        AND is_current = TRUE
        AND channel_name = 'Total Banco'
        AND channel_description = 'Total aggregate without segmentation by channel'
);

COMMIT;
