UPDATE meta.pipeline_watermarks
SET last_year = :last_year
WHERE pipeline_name = :pipeline_name