BEGIN
CREATE TEMP TABLE
    _SESSION.tmpExportTable AS (
        SELECT
            *
        FROM
            `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['store_visits_daily_table'] }}`
        WHERE
            local_date <= '{{ ds }}'
    );

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_90/finance/csv_format/historical/*.csv.gz",
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
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_90/finance/parquet_format/historical/*.zstd.parquet",
    FORMAT = 'PARQUET',
    compression = 'ZSTD',
    overwrite = TRUE
) AS
SELECT
    *
FROM
    _SESSION.tmpExportTable;

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_90/finance/gzip_parquet_format/historical/*.parquet.gz",
    FORMAT = 'PARQUET',
    compression = 'GZIP',
    overwrite = TRUE
) AS
SELECT
    *
FROM
    _SESSION.tmpExportTable;

END;

-- 90 DAYS FORECAST
BEGIN
CREATE TEMP TABLE
    _SESSION.tmpExportTable1 AS (
        SELECT
            *
        FROM
            `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['store_visits_daily_table'] }}`
        WHERE
            local_date <= DATE_ADD('{{ ds }}', INTERVAL 90 DAY)
            AND local_date > '{{ ds }}'
    );

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_90/finance/csv_format/forecasted/*.csv.gz",
    FORMAT = 'CSV',
    compression = 'GZIP',
    overwrite = TRUE,
    header = TRUE
) AS
SELECT
    *
FROM
    _SESSION.tmpExportTable1;

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_90/finance/parquet_format/forecasted/*.zstd.parquet",
    FORMAT = 'PARQUET',
    compression = 'ZSTD',
    overwrite = TRUE
) AS
SELECT
    *
FROM
    _SESSION.tmpExportTable1;

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
    uri = "gs://{{ params['feeds_staging_gcs_daily_bucket'] }}/export_date={{ next_ds.replace('-', '') }}/type_1/{{ params['store_visits_daily_table'].replace('_','-') }}/historical_90/finance/gzip_parquet_format/forecasted/*.parquet.gz",
    FORMAT = 'PARQUET',
    compression = 'GZIP',
    overwrite = TRUE
) AS
SELECT
    *
FROM
    _SESSION.tmpExportTable1;

END;