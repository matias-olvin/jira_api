EXPORT DATA OPTIONS(
  uri="gs://{{ params['dewey_bucket'] }}/{{ params['placekeys_table'] }}/{{ next_ds.replace('-', '/') }}/*.csv",
  format='CSV',
  overwrite=true,
  header=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['placekeys_table'] }}`