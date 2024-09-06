CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['prior_distances_table'] }}` AS
WITH
  -- places
  footprints_places AS (
  SELECT
    pid AS fk_sgplaces,
    ST_GEOGPOINT(longitude, latitude) as point,
    polygon_wkt 
  FROM
    --`storage-prod-olvin-com.sg_places.20211101`),
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`),
  footprints_places_2 as (
      select *  from footprints_places
  ),
  places_nearby AS (
  SELECT
    footprints_places.*
  FROM
    footprints_places
  INNER JOIN
    footprints_places_2
  ON
    ST_DWITHIN(footprints_places.point,
      footprints_places_2.point,
      400) ),
  -- we want only those places which have only their copy nearby
  isolated_places as (
    select fk_sgplaces
    from (select count(*) as count, fk_sgplaces from places_nearby group by fk_sgplaces)
    where count = 1
  ),
  device_visits as (
       select
        lat_long_visit_point,
        local_date
       from
       `{{ var.value.env_project }}.{{ params['device_visits_dataset'] }}.*`
       --`storage-prod-olvin-com.device_visits.*`
       where local_date in (
        CAST("{{ params['local_date_start_distance'] }}" AS DATE),
        --CAST('2020-01-05' AS DATE),
        CAST("{{ params['local_date_final_distance'] }}" AS DATE)
        --CAST('2020-10-05' AS DATE)
        )
  ),
  visits_nearby as (
    select *
    from footprints_places
    inner join isolated_places using (fk_sgplaces)
    inner join device_visits 
    ON
    ST_DWITHIN(footprints_places.point,
      device_visits.lat_long_visit_point,
      250) 
  ),
  visits_distances AS (
  SELECT
    local_date,
    ST_DISTANCE(point,
      lat_long_visit_point) AS distance_visits_centroid,
  FROM
    visits_nearby )
SELECT distinct *
FROM visits_distances