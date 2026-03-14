INSERT INTO etl_step_metrics (
    pipeline_name,
    step_name,
    rows_processed
)
VALUES (
    :pipeline_name,
    :step_name,
    :rows
)
RETURNING id
