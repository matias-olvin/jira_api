EXPORT DATA OPTIONS(
  uri="gs://{{ params['dewey_bucket'] }}/{{ params['store_visits_trend_table'].replace('_','-') }}/{{ next_ds.replace('-', '/') }}/*.csv",
  format='CSV',
  overwrite=true,
  header=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_trend_table'] }}`
  WHERE 
    month_starting < DATE_TRUNC("{{ next_ds }}", MONTH)
