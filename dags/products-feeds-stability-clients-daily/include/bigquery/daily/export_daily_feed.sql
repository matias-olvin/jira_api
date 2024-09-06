BEGIN
CREATE TEMP TABLE
    _SESSION.tmpExportTable AS (
        SELECT
            *
        FROM
            `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_daily_table'] }}`
        WHERE
            local_date = "{{ ds }}"
    );

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_2/{{ params['store_visits_daily_table'].replace('_','-') }}/general/csv_format/daily/*.csv.gz",
    FORMAT = 'CSV',
    compression = 'GZIP',
    overwrite = TRUE,
    header = TRUE
) AS
SELECT
    *
FROM
    _SESSION.tmpExportTable;

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_2/{{ params['store_visits_daily_table'].replace('_','-') }}/general/parquet_format/daily/*.zstd.parquet",
    FORMAT = 'PARQUET',
    compression = 'ZSTD',
    overwrite = TRUE
) AS
SELECT
    *
FROM
    _SESSION.tmpExportTable;

END;