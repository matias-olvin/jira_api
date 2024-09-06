CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['daily_visits_table'] }}`
  PARTITION BY local_date
  CLUSTER BY fk_sgplaces
AS
 WITH

poi_data AS(
  SELECT pid AS fk_sgplaces, 
         fk_sgbrands,
         ST_GEOGPOINT(longitude, latitude) AS poi_geoloc,
         IFNULL(opening_date, DATE('2019-01-01')) AS opening_date,
         polygon_area_sq_ft
  FROM `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}`
),

-- Get TREND

brand_pois AS(
  SELECT poi_to_add, 
         poi_opening,
         pid AS fk_sgplaces, 
         fk_sgbrands,
         polygon_area_sq_ft AS area 
  FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  INNER JOIN(
    SELECT fk_sgplaces AS poi_to_add, fk_sgbrands, opening_date AS poi_opening
    FROM poi_data
  )
  USING(fk_sgbrands)
),

get_visits_brand AS(
  SELECT *
  FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}`
  INNER JOIN
    brand_pois
  USING(fk_sgplaces)
  WHERE local_date >= poi_opening
),

-- explode the visits
daily_visits_brand AS(
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
    CAST(visits as INT64) visits,
    fk_sgplaces
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM get_visits_brand
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array as another column
  ORDER BY
    local_date,
    fk_sgplaces,
    row_number
),

average_visits_brand AS(
  SELECT fk_sgplaces, AVG(visits) AS avg_visits
  FROM daily_visits_brand
  GROUP BY fk_sgplaces
),

-- Trend of the POI: avg trend of its brand within the timeframe when it has been opened
trend_poi AS(
  SELECT poi_to_add, local_date, avg(adim_visits) AS trend
  FROM(
    SELECT fk_sgplaces, local_date, visits/avg_visits AS adim_visits, brand_pois.poi_to_add
    FROM daily_visits_brand
    INNER JOIN average_visits_brand
    USING(fk_sgplaces)
    INNER JOIN brand_pois
    USING (fk_sgplaces)
  )
  GROUP BY poi_to_add, local_date
),


-- Get VOLUME

nearby_pois AS(
  SELECT poi_to_add, pid AS fk_sgplaces, polygon_area_sq_ft AS area, fk_sgbrands
  FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` a
  INNER JOIN(
    SELECT fk_sgplaces AS poi_to_add, poi_geoloc
    FROM poi_data
  )
  ON ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), poi_geoloc) < 500
  -- Include condition so if less than 20 pois within 500 m we use 3km radius
),

brand_nearby_pois_for_volume AS(
  SELECT pid AS fk_sgplaces, fk_sgbrands, polygon_area_sq_ft AS area 
  FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  WHERE fk_sgbrands IN(
    SELECT fk_sgbrands
    FROM nearby_pois
  )
),

metric_nearby_pois_volume AS(
  SELECT fk_sgplaces, fk_sgbrands, avg_visits/POW(area, (1/2)) as mod_visits -- Change exponent of area if needed
  FROM(
    SELECT fk_sgplaces, AVG(visits) AS avg_visits
    FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplace_monthly_visits_raw'] }}`
    GROUP BY fk_sgplaces
  )
  INNER JOIN (
    SELECT fk_sgplaces, fk_sgbrands, IFNULL(IFNULL(area, brand_area), avg_global_area) AS area
    FROM brand_nearby_pois_for_volume
    LEFT JOIN(
      SELECT fk_sgbrands, AVG(area) AS brand_area
      FROM brand_nearby_pois_for_volume
      GROUP BY fk_sgbrands
    )
    USING(fk_sgbrands)
    LEFT JOIN(
      SELECT AVG(polygon_area_sq_ft) AS avg_global_area
      FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    )
    ON TRUE
  )
  USING(fk_sgplaces)
),

nearby_pois_perc AS (
  SELECT poi_to_add, fk_sgplaces, percentile
  FROM(
    SELECT fk_sgplaces, fk_sgbrands, mod_visits,
        PERCENT_RANK() OVER (PARTITION BY fk_sgbrands ORDER BY mod_visits)*100 AS percentile
    FROM metric_nearby_pois_volume
  )
  INNER JOIN nearby_pois
  USING(fk_sgplaces)
),

-- Percentile of the poi metric within its brand, based on nearby pois
nearby_based_perc AS (
  SELECT poi_to_add, AVG(percentile) AS perc_new_poi
  FROM nearby_pois_perc
  GROUP BY poi_to_add
),


-- Get corresponding percentile within the new poi brand

metric_brand_pois_volume AS(
  SELECT fk_sgplaces, fk_sgbrands, avg_visits/POW(area, (1/2)) as mod_visits -- Change exponent of area if needed
  FROM(
    SELECT fk_sgplaces, AVG(visits)*12/365 AS avg_visits
    FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplace_monthly_visits_raw'] }}`
    GROUP BY fk_sgplaces
  )
  INNER JOIN (
    SELECT fk_sgplaces, fk_sgbrands, IFNULL(IFNULL(area, brand_area), avg_global_area) AS area
    FROM brand_pois
    LEFT JOIN(
      SELECT fk_sgbrands, AVG(area) AS brand_area
      FROM brand_pois
      GROUP BY fk_sgbrands
    )
    USING(fk_sgbrands)
    LEFT JOIN(
      SELECT AVG(polygon_area_sq_ft) AS avg_global_area
      FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    )
    ON TRUE
  )
  USING(fk_sgplaces)
),

brand_pois_perc AS (
  SELECT fk_sgplaces, fk_sgbrands, mod_visits,
      PERCENT_RANK() OVER (PARTITION BY fk_sgbrands ORDER BY mod_visits)*100 AS percentile
  FROM metric_brand_pois_volume
),


-- Volume of the poi: making the rank of the modified visits within its brand equal to the one of nearby pois
volume_poi AS(
  SELECT poi_to_add, 
        ( lower_bound + (upper_bound-lower_bound)/(upper_perc-lower_perc)*(perc_new_poi-lower_perc) ) * POW(polygon_area_sq_ft, (1/2)) AS volume
  FROM (
    SELECT fk_sgplaces AS poi_to_add,
          polygon_area_sq_ft
    FROM poi_data
  )
  INNER JOIN nearby_based_perc
  USING(poi_to_add)
  INNER JOIN(
    SELECT 
          poi_to_add,
          MAX(mod_visits) AS lower_bound, 
          MAX(percentile) AS lower_perc
    FROM brand_pois_perc
    INNER JOIN brand_pois
    USING(fk_sgbrands)
    INNER JOIN nearby_based_perc
    USING(poi_to_add)
    WHERE percentile < perc_new_poi
    GROUP BY poi_to_add
  )
  USING(poi_to_add)
  INNER JOIN(
    SELECT 
          poi_to_add,
          MAX(mod_visits) AS upper_bound, 
          MAX(percentile) AS upper_perc
    FROM brand_pois_perc
    INNER JOIN brand_pois
    USING(fk_sgbrands)
    INNER JOIN nearby_based_perc
    USING(poi_to_add)
    WHERE percentile > perc_new_poi
    GROUP BY poi_to_add
  )
  USING(poi_to_add)
),

-- Daily Visits
daily_visits_table AS(
  SELECT poi_to_add AS fk_sgplaces, 
         local_date, 
         CAST(trend * volume AS INT64) AS visits
  FROM trend_poi
  INNER JOIN volume_poi
  USING(poi_to_add)
)

SELECT daily_visits_table.*
FROM daily_visits_table
INNER JOIN poi_data
USING(fk_sgplaces)
WHERE local_date >= opening_date