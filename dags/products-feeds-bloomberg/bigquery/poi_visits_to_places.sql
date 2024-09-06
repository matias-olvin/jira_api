WITH poi_visits_and_places as (
  SELECT 
    visit_ts as visit_start_ts, 
    TIMESTAMP_ADD(visit_ts, 
    INTERVAL duration SECOND) as visit_ts_end, 
    device_id, 
    device_os,
    duration, 
    local_date, 
    local_hour, 
    day_of_week, 
    visit_score.original as visit_score, 
    visit_score.opening as visit_score_opening,
    ST_Y(lat_long_visit_point) as lat_visit_point, 
    ST_X(visits.lat_long_visit_point) as long_visit_point, 
    fk_sgplaces as placekey_olvin, 
    street_address, 
    city, 
    region, 
    name, 
    latitude as poi_lat,
    longitude as poi_long,
    visits.fk_sgbrands,
    places.naics_code,
    places.top_category,
    places.sub_category,
    lat_long_visit_point
  FROM
  -- `storage-prod-olvin-com.poi_visits.2022` visits
    `{{ params['project'] }}.{{ params['poi_visits_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y')}}` visits

  JOIN (
    SELECT * 
    FROM
      `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{params['sg_places_table']}}`
    WHERE
      sensitive IS FALSE
  ) places
  ON 
    visits.fk_sgplaces = places.pid
  --  WHERE visits.local_date = '2022-02-16' 
  WHERE visits.local_date = "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}"
  -- and visits.fk_met_areas != 1000
),
join_met_areas as (
SELECT a.* EXCEPT(lat_long_visit_point), b.fk_met_areas FROM poi_visits_and_places a
left JOIN
  `{{ params['project'] }}.{{ params['static_match_dataset'] }}.{{ params['met_areas_table'] }}` b
  -- `storage-prod-olvin-com.static_match_data.met_areas_` b
ON
  st_INTERSECTS(a.lat_long_visit_point,
    b.geometry_wkt  )
),
join_brands as (
  SELECT 
    join_met_areas.* EXCEPT(fk_sgbrands), b.name as brand_name 
  FROM  
    join_met_areas 
  LEFT JOIN
  -- `storage-prod-olvin-com.sg_places.brands` b
    `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{ params['brand_table'] }}`b
  ON join_met_areas.fk_sgbrands = b.pid
),
olvin_to_placekey_map AS (
  SELECT 
    dynamic.pid AS olvin_id, 
    static.pid AS placekey 
  FROM 
    -- `storage-prod-olvin-com.sg_places.20211101` b
    `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{ params['static_places_table'] }}` static
  INNER JOIN 
    -- `storage-prod-olvin-com.sg_places.places_history` b
    `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{params['sg_places_table']}}` dynamic
  ON static.pid = dynamic.sg_id
)
SELECT 
  join_brands.* EXCEPT(placekey_olvin), 
  map.placekey AS placekey 
FROM join_brands
LEFT JOIN olvin_to_placekey_map map
ON map.olvin_id = join_brands.placekey_olvin
