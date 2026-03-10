SELECT MAX(end_time)
FROM meta.etl_runs
WHERE
    pipeline_name =: pipeline_name
    AND status = 'success'
