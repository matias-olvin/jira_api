create or replace table 
-- `storage-prod-olvin-com.lincoln_place_2.void_category` as
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['void_category_table'] }}` AS

WITH
category_pois as (
select pid as fk_sgplaces, name, street_address, region, postal_code, top_category,  sub_category, long_lat_point, ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( "MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816 38.58793708951073, -89.98786913032 38.5857063219599, -89.9852753964179 38.5856842631249, -89.98527219639965 38.59150426986252)")), 10) as polygon_site from `storage-prod-olvin-com.sg_places.20211101` 
where sub_category in (select sub_category from 
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['psychographics_category_table'] }}`)
-- `storage-prod-olvin-com.lincoln_place_2.pyschographics_category`)

),
all_closest_pois as (
select *, ST_DISTANCE(long_lat_point, polygon_site) site_poi_distance 
from category_pois 
order by site_poi_distance
),
closest_poi as (
SELECT * except(row_num) from (select fk_sgplaces,name, street_address, region, postal_code, top_category,  sub_category,  site_poi_distance, all_closest_pois.long_lat_point,
ROW_NUMBER() OVER (partition by sub_category order by site_poi_distance) row_num 
from all_closest_pois )
where row_num =1 

)
  
select * from closest_poi order by sub_category