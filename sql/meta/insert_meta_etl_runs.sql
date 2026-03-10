INSERT INTO meta.etl_runs (
    pipeline_name,
    start_time,
    status
)
VALUES (
    : pipeline_name,
    NOW(),
    'running'
)
RETURNING run_id;
