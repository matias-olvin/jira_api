EXPORT DATA OPTIONS(
    uri = "gs://{{ params['versioning_bucket'] }}/type-2/{{ params['store_visits_daily_table'] }}/{{ next_ds.replace('-', '/') }}/*.zstd.parquet",
    FORMAT = 'PARQUET',
    compression = 'ZSTD',
    overwrite = TRUE
) AS
SELECT
    *
FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_daily_table'] }}`
WHERE local_date >= DATE_TRUNC('{{ ds }}', MONTH);