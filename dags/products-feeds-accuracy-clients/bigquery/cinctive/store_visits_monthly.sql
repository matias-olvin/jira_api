EXPORT DATA OPTIONS(
  uri="gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_table'].replace('_','-') }}/historical/*.csv.gz",
  format='CSV',
  compression='GZIP',
  overwrite=true,
  header=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['store_visits_table'] }}`
WHERE 
  week_ending < DATE_TRUNC("{{ next_ds }}", WEEK);

EXPORT DATA OPTIONS(
  uri="gs://{{ params['feeds_staging_gcs_no_pays_bucket'] }}/cinctive/export_date={{ next_ds.replace('-', '') }}/{{ params['store_visits_table'].replace('_','-') }}/weekly_forecast/*.csv.gz",
  format='CSV',
  compression='GZIP',
  overwrite=true,
  header=true) AS
SELECT * FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['store_visits_table'] }}`
WHERE 
-- The < condition checks that the date is less than 60 days after the actual NEXT execution date of this Airflow
-- pipeline. Airflow does not have a built-in param for this so we add a week corresponding to the schedule
-- and then 60 days
  week_starting < DATE_ADD(DATE_ADD(DATE_TRUNC("{{ next_ds }}", WEEK), INTERVAL 1 WEEK), INTERVAL 60 DAY)
  AND week_ending >= DATE_TRUNC("{{ next_ds }}", WEEK);