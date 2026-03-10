UPDATE meta.etl_runs
SET
    end_time = NOW(),
    status = 'success',
    rows_loaded =: rows_loaded
WHERE run_id =: run_id
