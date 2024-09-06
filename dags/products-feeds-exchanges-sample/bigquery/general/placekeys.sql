EXPORT DATA OPTIONS(
  uri="gs://{{ params['sample_bucket'] }}/general/{{ params['placekeys_table'] }}/*.zstd.parquet",
  format='PARQUET',
  compression='ZSTD'
  ) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['placekeys_table'] }}`