-- Increase the size of trade_area=30 polygons
UPDATE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}`
SET polygon = ST_ASTEXT(ST_BUFFER(ST_SIMPLIFY(ST_GEOGFROMTEXT(polygon),50), 100, 8))
WHERE trade_area = 30
    AND local_date=DATE("{{ ds.format('%Y-%m-01') }}")
AND ST_AREA(ST_GEOGFROMTEXT(polygon)) < 31420;
 
-- Increase the size of trade_area=30 polygons
UPDATE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}`
SET polygon = ST_ASTEXT(ST_BUFFER(ST_SIMPLIFY(ST_GEOGFROMTEXT(polygon),50), 120, 8))
WHERE trade_area = 50
AND local_date=DATE("{{ ds.format('%Y-%m-01') }}")
AND ST_AREA(ST_GEOGFROMTEXT(polygon)) < 31420; 

-- Increase the size of trade_area=30 polygons
UPDATE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}` 
SET polygon = ST_ASTEXT(ST_BUFFER(ST_SIMPLIFY(ST_GEOGFROMTEXT(polygon),50), 150, 8))
WHERE trade_area = 70
AND local_date=DATE("{{ ds.format('%Y-%m-01') }}")
AND ST_AREA(ST_GEOGFROMTEXT(polygon)) < 45240;