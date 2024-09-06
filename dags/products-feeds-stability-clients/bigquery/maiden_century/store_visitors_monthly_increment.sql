EXPORT DATA OPTIONS(
  uri="gs://{{ params['feeds_staging_gcs_bucket'] }}/maiden_century/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visitors_table'].replace('_','-') }}/monthly/*.zstd.parquet",
  format='PARQUET',
  compression='ZSTD',
  overwrite=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['store_visitors_table'] }}`
  WHERE
  month_starting < DATE_TRUNC("{{ next_ds }}", MONTH)
  AND month_starting >= DATE_TRUNC("{{ ds }}", MONTH)