LOAD DATA OVERWRITE
  `{{ var.value.env_project }}.{{ params['static_features_staging_dataset'] }}.{{ params['dataforseo_table'] }}`
  (
    fk_sgplaces STRING
    , rating FLOAT64
    , seo_count FLOAT64
  )
  FROM FILES (
    format = 'CSV'
    , skip_leading_rows = 1
    , uris = ["gs://{{ params['dataforseo-bucket'] }}/{{ params['ENDPOINT'] }}/ingestion_date={{ ds }}/{{ params['PREFIX'] }}/batch={{ '%012d' % dag_run.conf['batch-number'] }}/*.csv"]
  );


