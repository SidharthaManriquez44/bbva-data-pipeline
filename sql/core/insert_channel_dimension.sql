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
    FROM core.dim_channel AS dc
    WHERE
        dc.channel_code = 'TOTAL'
        AND dc.effective_from = CURRENT_DATE
);
