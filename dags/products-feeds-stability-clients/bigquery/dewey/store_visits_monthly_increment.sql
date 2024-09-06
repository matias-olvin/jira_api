EXPORT DATA OPTIONS(
  uri="gs://{{ params['dewey_bucket'] }}/{{ params['store_visits_table'].replace('_','-') }}/{{ next_ds.replace('-', '/') }}/*.csv",
  format='CSV',
  overwrite=true,
  header=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_table'] }}`
WHERE 
  week_ending <= DATE_TRUNC("{{ next_ds }}", WEEK)
  AND week_ending >= DATE_TRUNC("{{ ds }}", MONTH);
-- This works for the majority of the months, but June for example will include an overlap week. This is better than missing out a week every month.
-- a solution could be to obtain the last first monday and use that as a reference point for when to cut off the data.