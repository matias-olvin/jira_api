CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['chain_ranking_local_table'] }}` as

WITH 
 place_ids as (
SELECT
fk_sgplaces
-- FROM (select ['222-222@5pj-5nd-wkz','222-222@5pj-5nd-w8v','224-222@5pj-5nd-syv','zzw-222@5pj-5nd-syv','223-222@5pj-5nd-syv','224-222@5pj-5nd-snq','223-222@5pj-5nd-snq','222-223@5pj-5nd-snq']
FROM ( SELECT {{ dag_run.conf['place_ids'] }}
as fk_sgplaces),
UNNEST(fk_sgplaces) fk_sgplaces

),
get_brands as (
 SELECT distinct fk_sgbrands   
 from `{{ var.value.storage_project_id }}.{{ params['places_dataset']  }}.{{ params['places_table'] }}`
--  from `storage-prod-olvin-com.sg_places.20211101` 
 where pid in (select fk_sgplaces from place_ids) 
),
get_site_pois as (
 SELECT pid, fk_sgbrands, name, True as site_location, longitude, latitude, city, top_category, region, street_address, postal_code  
 from `{{ var.value.storage_project_id }}.{{ params['places_dataset']  }}.{{ params['places_table'] }}`
  where fk_sgbrands in (select fk_sgbrands from get_brands) 
and
--  ST_Distance(long_lat_point , ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( "MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816 38.58793708951073, -89.98786913032 38.5857063219599, -89.9852753964179 38.5856842631249, -89.98527219639965 38.59150426986252)")), 10)) <= 24140.2
 ST_DWITHIN(long_lat_point , {{ dag_run.conf['select_geometry'] }}, 24140.2)

),

-- join the visits - from ground truth volume model
get_visits as (
  SELECT a.*, b.monthly_visits FROM get_site_pois a
  LEFT JOIN
  `{{ var.value.storage_project_id }}.{{ params["ground_truth_volume_dataset"] }}.{{ params['ground_truth_volume_table'] }}` b
--   `storage-prod-olvin-com.ground_truth_volume_model.met_area_poi_monthly_visits` b
  on a.pid = b.fk_sgplaces
  where monthly_visits is not null
),

-- rank the visits of each poi compared to other pois in the brand
rank_visits_by_brand as (
  SELECT *, RANK() OVER (PARTITION BY fk_sgbrands ORDER BY monthly_visits desc) as rank, PERCENT_RANK() OVER (PARTITION BY fk_sgbrands ORDER BY monthly_visits desc) as percent_rank,
  COUNT(name) OVER (PARTITION BY fk_sgbrands) as poi_count
  FROM get_visits
),

get_output as (
select pid as fk_sgplaces, fk_sgbrands, name, region, street_address, postal_code, longitude, latitude, city, top_category, 100-max(percent_rank*100) current_percent_rank, max(rank) as rank, max(poi_count ) poi_count, max(monthly_visits) visits  from rank_visits_by_brand 
group by 1,2,3,4,5,6,7,8,9,10 order by rank
),

join_area_polygon as (
select a.*, ST_AREA(b.polygon_wkt) as polygon_area  from get_output a
left join
 `{{ var.value.storage_project_id }}.{{ params['places_dataset']  }}.{{ params['places_table'] }}` b
on a.fk_sgplaces = b.pid

),
get_visits_per_sq_ft as (
  select *, visits/(polygon_area*10.7639) as visits_per_sq_ft
  from join_area_polygon
),
get_percentile_rank_visits_per_sq_ft as (
select *, 100-((PERCENT_RANK() OVER (PARTITION BY fk_sgbrands ORDER BY visits_per_sq_ft desc))*100) as percent_rank_visits_per_sq_ft
from get_visits_per_sq_ft 
  
)
select * from get_percentile_rank_visits_per_sq_ft

