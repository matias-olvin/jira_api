LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_geometries_table'] }}_test`
  CLUSTER BY fk_sgplaces
  FROM FILES(
    format='PARQUET',
    uris = ["gs://{{ params['trade_area_bucket'] }}/{{ ds_nodash.format('%Y%m01') }}/test/output/*"]
  )