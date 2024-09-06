EXPORT DATA OPTIONS(
  uri="gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['placekeys_table'] }}/historical/*.csv.gz",
  format='CSV',
  compression='GZIP',
  overwrite=true,
  header=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['placekeys_table'] }}`