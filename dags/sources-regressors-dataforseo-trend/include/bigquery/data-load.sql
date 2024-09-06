LOAD DATA OVERWRITE
  `{{ var.value.env_project }}.{{ params['regressors_staging_dataset'] }}.{{ params['web_search_trend_table'] }}`
  (
    fk_sgbrands STRING
    , local_date DATE
    , value FLOAT64
    , type STRING
  )
  FROM FILES (
    format = 'CSV'
    , skip_leading_rows = 1
    , uris = ['gs://{{ params["dataforseo-bucket"] }}/{{ params["ENDPOINT"] }}/ingestion_date={{ ds }}/{{ params["PREFIX"] }}.csv']
  );


