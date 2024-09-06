DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

EXPORT DATA OPTIONS(
  uri=CONCAT('gs://{{ params["gcs-bucket"] }}/instance={{ params["db"] }}/{{ params["table"] }}/local_date=', date_to_update, '/*.csv.gzip')
  , format='CSV'
  , header = true
  , field_delimiter = '\t'
  , compression='GZIP'
  , overwrite=TRUE
) AS (
  SELECT *
  FROM `{{ params['bigquery-project'] }}.{{ params['dataset_bigquery'] }}.{{ params['table'] }}`
  WHERE local_date = date_to_update
);