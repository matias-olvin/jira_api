CREATE TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_table'] }}` (
    fk_sgplaces STRING,
    polygon GEOGRAPHY,
    trade_area INT,
    local_date DATE
   
)
cluster by fk_sgplaces;

TRUNCATE TABLE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_table'] }}`;

INSERT into  `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_table'] }}`
SELECT *, 30 AS trade_area, DATE("{{ ds.format('%Y-%m-01') }}") as local_date
FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_30_table'] }}`

union all
SELECT *, 50 AS trade_area, DATE("{{ ds.format('%Y-%m-01') }}") as local_date
FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_50_table'] }}`

union all
SELECT *, 70 AS trade_area, DATE("{{ ds.format('%Y-%m-01') }}") as local_date
FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_70_table'] }}`;