-- Range of dates we will use as input data of the month
WITH
  -- could be used
  date_range AS (
    SELECT
      DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)  AS date_begin_data,
      CURRENT_DATE() AS date_end_data
  ),


  visits_to_poi AS (
    SELECT
      device_id,
      TIMESTAMP(visit_ts) AS visit_ts,
      fk_sgplaces,
      visit_score,
      lat_long_visit_point AS point,
      duration,
      ifnull(dummy, False) as dummy
    --FROM `storage-dev-olvin-com.poi_visits_scaled.2022`
    FROM 
      `{{ params['project'] }}.{{ params['poi_visits_dataset'] }}.*`

    inner join 
      (select distinct pid as fk_sgplaces from
          --`storage-dev-olvin-com.postgres.SGPlaceRaw`
          `{{ params['project'] }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
      ) using (fk_sgplaces)
    left join 
      (select distinct  fk_sgplaces, True as dummy from 
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['sample_poi_list_table'] }}`
      ) using (fk_sgplaces)      

    WHERE
      local_date >= (SELECT date_begin_data FROM date_range)
    AND 
      local_date < (SELECT date_end_data FROM date_range)
    AND visit_score > 0
  ),

  poi_visits_filtering_if_sample as 
  (
    select * except (dummy)
    from visits_to_poi
    where dummy = True or {{ is_sample }} = False
  ),


  -- To detect visits to home
  -- visits_to_latlong AS (
  --   SELECT 
  --     device_id,
  --     fk_met_areas,
  --     TIMESTAMP(visit_ts) AS visit_ts,
  --     duration,
  --     lat_long_visit_point AS point,
  --   --FROM `storage-dev-olvin-com.sg_networks_staging.device_visits_sample`
  --   FROM `{{ params['project'] }}.{{ params['device_visits_historical_dataset'] }}.*`
  --   WHERE
  --     local_date >= (SELECT date_begin_data FROM date_range)
  --   AND 
  --     local_date <= (SELECT date_end_data FROM date_range)
  -- ),
  
  device_home_locations AS (
   SELECT 
      device_id,
      home_location
   FROM (
   SELECT 
      device_id, 
      home_point AS home_location,
      ROW_NUMBER() OVER(PARTITION BY device_id ORDER BY local_date DESC) AS row_num
    FROM 
       --`storage-dev-olvin-com.device_homes.2022`
       `{{ params['project'] }}.{{ params['device_homes_dataset'] }}.*`
    WHERE
      local_date >= (SELECT date_begin_data FROM date_range)
    AND 
      local_date < (SELECT date_end_data FROM date_range)
       AND home_point IS NOT NULL
       )
    WHERE
      row_num = 1
  ),
  
  visits_poi_add_homes AS (
    SELECT
      poi_visits_filtering_if_sample.*,
      ST_Distance(poi_visits_filtering_if_sample.point, device_home_locations.home_location) AS distance_from_home
    FROM
      poi_visits_filtering_if_sample
    LEFT JOIN
      device_home_locations
    ON
      poi_visits_filtering_if_sample.device_id = device_home_locations.device_id
  ),
  
  -- visits_latlong_add_homes AS (
  --   SELECT
  --     visits_to_latlong.*,
  --     ST_Distance(visits_to_latlong.point, device_home_locations.home_location) AS distance_from_home
  --   FROM
  --     visits_to_latlong
  --   LEFT JOIN
  --     device_home_locations
  --   ON
  --     visits_to_latlong.device_id = device_home_locations.device_id
  -- ),
  
  visits_poi_filter_visitors AS (
    SELECT
      device_id,
      visit_ts,
      duration,
      fk_sgplaces,
      visit_score,
      point,
      CASE
        WHEN row_num = 1 THEN True
        ELSE False
      END AS use_in_connections
    FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY device_id, fk_sgplaces ORDER BY visit_score DESC) AS row_num
      FROM
        visits_poi_add_homes   
      WHERE 
        --distance_from_home > CAST("30" AS INT64) 
        distance_from_home > CAST("{{params['distance_home_threshold']}}" AS INT64) 
      OR 
        distance_from_home IS NULL )
  ),
  
  -- visits_latlong_filter AS (
  --   SELECT
  --     *
  --   FROM
  --     visits_latlong_add_homes
  --   WHERE
  --     distance_from_home < CAST("{{params['distance_home_threshold']}}" AS INT64) 
  -- ),
  
  all_visits AS (
    SELECT
      device_id,
      visit_ts,
      duration,
      fk_sgplaces AS fk_nodes,
      visit_score,
      point,
      use_in_connections,
      DATE("{{ ds.format('%Y-%m-01') }}") AS local_date
    FROM
      visits_poi_filter_visitors
    -- the below code is for latlong_filter
    -- UNION ALL
    -- SELECT
    --   device_id,
    --   fk_met_areas,
    --   visit_ts,
    --   duration,
    --   'home' AS fk_nodes,
    --   1 AS visit_score,
    --   point,
    --   False AS use_in_connections,
    --   IF ("{{params['mode']}}" = "historical",
    --       PARSE_DATE('%F', "{{ ds.format('%Y-%m-01') }}"),
    --       --DATE_TRUNC(DATE_SUB( CAST(CURRENT_DATE() AS DATE), INTERVAL 0 MONTH), MONTH)
    --         DATE_TRUNC(DATE_SUB( CAST( PARSE_DATE('%F', "{{ ds.format('%Y-%m-01') }}") AS DATE), INTERVAL 1 MONTH), MONTH)
    --       ) AS local_date
    -- FROM 
    --   visits_latlong_filter
  )
     
SELECT 
  *
FROM
  all_visits
      
