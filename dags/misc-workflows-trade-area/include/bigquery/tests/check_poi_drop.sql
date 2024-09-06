DECLARE count_pois_orginal, count_pois_processed INT64 ;


SET count_pois_orginal = (
  SELECT count(distinct fk_sgplaces) from `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_table'] }}`
  WHERE local_date = DATE("{{ ds.format('%Y-%m-01') }}")
);


SET count_pois_processed = (
  SELECT count(distinct fk_sgplaces) from `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}`
  WHERE local_date = DATE("{{ ds.format('%Y-%m-01') }}")
);

ASSERT (count_pois_orginal-count_pois_processed)<500000;