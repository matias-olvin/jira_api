EXPORT DATA
  OPTIONS (
    uri = "gs://{{ params['trade_area_bucket'] }}/{{ ds_nodash.format('%Y%m') }}/test/input/*.parquet"
    , format = 'PARQUET'
    , OVERWRITE = TRUE
  ) AS (
    WITH distinct_places AS (
    SELECT fk_sgplaces
    FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['trade_area_input_table'] }}`
    GROUP BY fk_sgplaces
    LIMIT 10000
    )

    SELECT *
    FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['trade_area_input_table'] }}`
    WHERE fk_sgplaces IN (SELECT fk_sgplaces FROM distinct_places)
  );
