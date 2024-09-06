EXPORT DATA OPTIONS(
  uri="gs://{{ params['feeds_staging_gcs_bucket'] }}/twinbeech_capital/export_date={{ next_ds.replace('-', '') }}/{{ params['placekeys_table'] }}/historical/*.zstd.parquet",
  format='PARQUET',
  compression='ZSTD',
  overwrite=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['placekeys_table'] }}`