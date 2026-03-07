UPDATE meta.etl_runs
            SET end_time = NOW(),
                status = 'failed',
                error_message = :error_message
            WHERE run_id = :run_id