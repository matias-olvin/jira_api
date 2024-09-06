EXPORT DATA OPTIONS(
  uri="gs://{{ params['sample_bucket'] }}/general/{{ params['store_visits_trend_table'].replace('_','-') }}/*.zstd.parquet",
  format='PARQUET',
  compression='ZSTD'
  ) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_trend_table'] }}`