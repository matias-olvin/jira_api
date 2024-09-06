EXPORT DATA OPTIONS(
  uri="gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/broadbay/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_trend_table'].replace('_','-') }}/historical/*.zstd.parquet",
  format='PARQUET',
  compression='ZSTD',
  overwrite=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['store_visits_trend_table'] }}`
  WHERE 
    month_starting < DATE_TRUNC("{{ next_ds }}", MONTH)
