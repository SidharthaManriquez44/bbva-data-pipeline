SELECT last_year
FROM meta.pipeline_watermarks
WHERE pipeline_name = :pipeline_name
