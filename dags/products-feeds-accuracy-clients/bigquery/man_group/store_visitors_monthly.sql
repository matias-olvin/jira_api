BEGIN
CREATE TEMP TABLE
  _SESSION.tmpExportTable AS (
    SELECT
      *
    FROM
      `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visitors_table'] }}`
    WHERE
      month_starting < DATE_TRUNC("{{ next_ds }}", MONTH)
  );

-- ADD TEMP TABLE TO AVOID MULTIPLE EMPTY FILES
EXPORT DATA OPTIONS(
  uri = "gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/man_group/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/*.csv.gz",
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