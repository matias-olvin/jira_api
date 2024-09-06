EXPORT DATA
  OPTIONS (
    uri = "gs://{{ params['trade_area_bucket'] }}/{{ ds_nodash.format('%Y%m') }}/input/*.parquet"
    , format = 'PARQUET'
    , OVERWRITE = TRUE
  ) AS (
    SELECT *
    FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['trade_area_input_table'] }}`
  );
