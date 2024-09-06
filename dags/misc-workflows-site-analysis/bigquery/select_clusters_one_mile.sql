  --  create or replace table `storage-prod-olvin-com.lincoln_place_2.sample_visits_1_mile_buffer` as
CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_one_mile_table'] }}` AS

WITH
  target_geog AS (
  SELECT
    ST_BUFFER(ST_DIFFERENCE(polygon_site,
        remove_site),
      1609) AS polygon_site
  FROM 
  --  (SELECT ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( "MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816 38.58793708951073, -89.98786913032 38.5857063219599, -89.9852753964179 38.5856842631249, -89.98527219639965 38.59150426986252)")), 10) AS polygon_site,
    -- ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( "MULTIPOINT(-89.98803564984877 38.591715930276216, -89.98785003060104 38.58795376381923, -89.9897250862117 38.58791768651026)")), 5) AS remove_site)
        (SELECT {{ dag_run.conf['select_geometry'] }} AS polygon_site,
    {{ dag_run.conf['remove_geometry'] }} AS remove_site)
  ),
        
  get_device_to_site as (
SELECT
  device_id,
  lat_long_visit_point,
  duration,
  local_date,
  local_hour,
  1 AS visit_score
FROM (
  SELECT
    *
  FROM (
    SELECT
      device_id,
      lat_long_visit_point,
      duration,
      local_date,
      local_hour
    FROM
    -- `storage-prod-olvin-com.device_clusters.*`
     `{{ var.value.storage_project_id }}.{{ params['device_clusters_dataset'] }}.*`
    WHERE
    -- local_date >= '2021-01-01' AND local_date < "2022-01-01"
    -- and ST_WITHIN(lat_long_visit_point, ST_BUFFER(ST_CONVEXHULL(ST_GEOGFROMTEXT( "MULTIPOINT(-89.9876924171444 38.591447875199094, -89.98780475730166 38.588004179215616, -89.98841093653816 38.58793708951073, -89.98786913032 38.5857063219599, -89.9852753964179 38.5856842631249, -89.98527219639965 38.59150426986252)")), 1609) )
     local_date >= "{{ dag_run.conf['start_date_string'] }}" AND local_date < "{{ dag_run.conf['end_date_string'] }}"
      AND ST_WITHIN(lat_long_visit_point, ST_BUFFER( {{ dag_run.conf['select_geometry_no_buffer'] }}, 1609))
  )
  JOIN
    target_geog
  ON
    ST_CONTAINS(polygon_site,
      lat_long_visit_point))
      )

select * from get_device_to_site
