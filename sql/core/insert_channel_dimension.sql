INSERT INTO core.dim_channel (
    channel_code,
    channel_name,
    channel_description,
    effective_from,
    is_current
)
SELECT
    'TOTAL',
    'Total Banco',
    'Total aggregate without segmentation by channel',
    CURRENT_DATE,
    TRUE
WHERE NOT EXISTS (
    SELECT 1
    FROM core.dim_channel
    WHERE channel_code = 'TOTAL'
    AND effective_from = CURRENT_DATE
);
