create or replace table 
-- `storage-prod-olvin-com.lincoln_place_2.pyschographics_category` as
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['psychographics_category_table'] }}` AS


WITH
  get_devices AS (
  SELECT
    *
  FROM
  `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_one_mile_table'] }}` ),
    -- `storage-prod-olvin-com.lincoln_place_2.sample_visits_1_mile_buffer` ),
  get_pois_visited AS (
  SELECT
    CASE
      WHEN LENGTH(device_id) = 36 THEN TO_BASE64(SHA1(CONCAT(REPLACE(device_id, "-", ""), '62Ou6@tYZ&')))  # Unencrypted HEX
      WHEN LENGTH(device_id) = 24 THEN TO_BASE64(SHA1(CONCAT(TO_HEX(FROM_BASE64(device_id)), '62Ou6@tYZ&')))  # Unencrypted BASE64
      WHEN LENGTH(device_id) = 28 THEN device_id  # Encrypted BASE64
    ELSE
    device_id
  END
    AS device_id,
    fk_sgplaces,
    fk_sgbrands,
    lat_long_visit_point
  FROM
   `{{ var.value.storage_project_id }}.{{ params['poi_visits_dataset'] }}.*`
    -- `storage-prod-olvin-com.poi_visits.*`
  WHERE
    -- ST_INTERSECTS( ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( "MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816  38.58793708951073, -89.98786913032 38.5857063219599,   -89.9852753964179 38.5856842631249, -89.98527219639965     38.59150426986252)")) , 160934), # 100 miles
    --   lat_long_visit_point) and local_date = '2021-01-01'
    
  ST_INTERSECTS(ST_BUFFER( {{ dag_run.conf['select_geometry_no_buffer'] }}, 160934), lat_long_visit_point)
   and local_date >= DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY), INTERVAL 12 MONTH) and local_date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
    AND fk_sgbrands IS NOT NULL ),
    
    get_pois_visited_by_device as (
    select * from get_pois_visited where device_id in (select distinct device_id  from get_devices)
    ),
    group_places as (
select fk_sgplaces,  device_id, count(fk_sgplaces) total_visits_to_poi , count(distinct device_id) unique_device_to_poi from get_pois_visited_by_device group by fk_sgplaces,device_id
),
join_subcategory as (
select a.*, b.sub_category  from get_pois_visited_by_device a
left join 
 `{{ var.value.storage_project_id }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}` b
-- `storage-prod-olvin-com.sg_places.20211101`  b
on a.fk_sgplaces = b.pid 
),
group_category_1 as (
select sub_category,  device_id, count(sub_category) total_visits_to_categories , count(distinct device_id) unique_device_to_category from join_subcategory group by sub_category, device_id

),
group_category_2 as (
select sub_category,  count(sub_category) total_visits_to_categories , count(distinct device_id) unique_device_to_category from group_category_1 group by sub_category

)

select * from group_category_2
  
  
  