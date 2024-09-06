BEGIN
CREATE TEMP TABLE
  _SESSION.tmpExportTable AS (
    SELECT
      *
    FROM
      `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_trend_table'] }}`
    WHERE
      month_starting < DATE_TRUNC("{{ next_ds }}", MONTH)
  );

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
  uri = "gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/visionworks/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/*.parquet.gz",
  FORMAT = 'PARQUET',
  compression = 'GZIP',
  overwrite = TRUE
) AS
SELECT
  *
FROM
  _SESSION.tmpExportTable;

END;