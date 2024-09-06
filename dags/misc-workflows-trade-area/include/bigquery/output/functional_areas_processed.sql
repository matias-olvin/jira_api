DELETE FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}` 
WHERE local_date=DATE("{{ ds.format('%Y-%m-01') }}");

INSERT into `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}` 

SELECT t1.*except(polygon),st_astext(polygon) as polygon
FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['functional_areas_table'] }}` t1
INNER JOIN `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` t2
ON t1.fk_sgplaces=t2.pid
WHERE ST_Area(t1.polygon) < 5000000000000
AND ST_Distance(ST_Centroid(t1.polygon), ST_GEOGPOINT(t2.longitude,t2.latitude)) <= 500000;

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.bk-{{ params['processed_functional_areas_table'] }}`;

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.bk-{{ params['processed_functional_areas_table'] }}`
CLONE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}`;