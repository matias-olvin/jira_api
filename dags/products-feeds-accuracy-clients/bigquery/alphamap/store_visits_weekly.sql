BEGIN
CREATE TEMP TABLE
  _SESSION.tmpExportTable AS (
    SELECT
      *
    FROM
      `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_table'] }}`
    WHERE
      week_ending < DATE_TRUNC("{{ next_ds }}", WEEK)
      AND week_starting >= DATE_TRUNC("{{ ds }}", WEEK)
  );

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
  uri = "gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/alphamap/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_table'].replace('_','-') }}/*.csv.gz",
  FORMAT = 'CSV',
  compression = 'GZIP',
  overwrite = TRUE,
  header = TRUE
) AS
SELECT
  *
FROM
  _SESSION.tmpExportTable;

END;