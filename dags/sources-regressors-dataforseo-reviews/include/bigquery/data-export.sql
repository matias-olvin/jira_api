EXPORT DATA
  OPTIONS (
    uri = "gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/{{ params['PREFIX'] }}/batch={{ '%012d' % dag_run.conf['batch-number'] }}/*.csv"
    , format = 'CSV'
    , OVERWRITE = TRUE
    , header = TRUE
    , field_delimiter = ','
  ) AS (
    SELECT *
    FROM `{{ var.value.env_project }}.{{ params['static_features_staging_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    WHERE 
      row_num > {{ dag_run.conf['batch-size'] }} * ({{ dag_run.conf['batch-number'] }} - 1)
      AND row_num <= {{ dag_run.conf['batch-size'] }} * {{ dag_run.conf['batch-number'] }}
  );
