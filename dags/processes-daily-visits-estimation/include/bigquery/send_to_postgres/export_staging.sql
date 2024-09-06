DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

EXPORT DATA OPTIONS(
    uri='gs://{{ params["gcs-bucket"] }}/instance={{ params["db"] }}/{{ params["table"] }}/*.csv'
    , format='CSV'
    , header = true
    , field_delimiter = '\t'
    , overwrite=TRUE
) AS (
  SELECT *
  FROM `{{ params['bigquery-project'] }}.{{ params['dataset_bigquery'] }}.{{ params['table'] }}`
);