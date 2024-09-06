EXPORT DATA OPTIONS(
    uri = "gs://{{ params['versioning_bucket'] }}/type-2/{{ params['store_visits_trend_table'] }}/{{ next_ds.replace('-', '/') }}/*.zstd.parquet",
    FORMAT = 'PARQUET',
    compression = 'ZSTD',
    overwrite = TRUE
) AS
SELECT
    *
FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_trend_table'] }}`
WHERE
    month_starting >= DATE_TRUNC('{{ ds }}', MONTH)
    AND month_starting < DATE_TRUNC(DATE_ADD('{{ ds }}', INTERVAL 1 MONTH), MONTH);