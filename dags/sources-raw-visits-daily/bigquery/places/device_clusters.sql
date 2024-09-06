WITH
  drop_duplicates AS (
  SELECT k.*
  FROM (
    SELECT ARRAY_AGG(x limit 1)[offset(0)] k
    FROM   
    `{{ params['project'] }}.{{ params['device_geohits_dataset'] }}.*` x
    -- `storage-prod-olvin-com.device_geohits.*` x
    WHERE local_date = "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}"
    -- WHERE  local_date >= "2021-11-07" 
    GROUP BY sdk_ts, device_id, publisher_id)
 ),
  sample_data AS (
  SELECT unnested.* from (select unnested 
  FROM (
    SELECT ARRAY_AGG(x limit 400) k
    FROM drop_duplicates x
    GROUP BY device_id, timezone, local_date),
    UNNEST(k) unnested)
    )
 ,
  --ACCURACY
  geometries_table AS (
  SELECT
    timezone,
    local_date,
    local_hour,
    ST_GEOGPOINT(longitude,
      latitude) AS point,
    device_id,
    device_os,
    sdk_ts,
    latitude,
    longitude,
    UNIX_SECONDS(sdk_ts) AS hour_ts,
    accuracy,
    publisher_id,
    COUNT(device_id) OVER (PARTITION BY device_id, 
timezone, local_date) AS n_hits,
    country
    -- Here also all the other columns of the table that 
-- are going to be used in the future
  FROM
    sample_data )

    ,
  distances_table AS (
  SELECT
    *,
    ST_DISTANCE(point,
      LAG(point) OVER order_point) AS distance_prev,
    ST_DISTANCE(point,
      LEAD(point, 1) OVER order_point) AS distance_foll,
    ST_DISTANCE(LAG(point) OVER order_point,
      LEAD(point, 1) OVER order_point) AS distance_path,
    (1-(hour_ts - LAG(hour_ts) OVER order_point)/7200) AS 
prev_tau,
    (1-(LEAD(hour_ts, 1) OVER order_point - 
hour_ts)/7200) AS foll_tau,
  FROM
    geometries_table
  WINDOW
    order_point AS (
    PARTITION BY
      device_id
    ORDER BY
      hour_ts) ),
  nb_p_table AS (
  SELECT
    * EXCEPT(distance_prev),
    CASE
      WHEN (prev_tau > 0) AND (foll_tau > 0) AND 
(distance_prev+distance_foll > 0) THEN 
(distance_prev*prev_tau + 
distance_foll*foll_tau)/(prev_tau + 
foll_tau)*(1-SQRT(distance_path/(distance_prev+distance_foll)))
      WHEN (prev_tau > 0) THEN distance_prev*prev_tau
      WHEN (foll_tau > 0) THEN distance_foll*foll_tau
    ELSE
    NULL
  END
    AS nb_p_accuracy,
    CASE
      WHEN (prev_tau > 0) AND (foll_tau > 0) THEN 
(prev_tau + foll_tau)/2
      WHEN (prev_tau > 0) THEN prev_tau/2
      WHEN (foll_tau > 0) THEN foll_tau/2
    ELSE
    NULL
  END
    AS nb_p_confidence,
    CASE
      WHEN (prev_tau > 0) AND (distance_prev > 0) THEN 
distance_prev
    ELSE
    NULL
  END
    AS distance_prev,
    ST_ASTEXT(point) AS point_text,
  FROM
    distances_table),
  grid_table AS (
  SELECT
    point_text,
    COUNT(point_text) AS grid_hits,
    AVG(distance_prev) AS grid_accuracy,
    (1 - STDDEV(distance_prev)/AVG(distance_prev)) AS 
grid_confidence
  FROM
    nb_p_table
  GROUP BY
    point_text),
  final_quality_creation AS (
  SELECT
    *,
    CASE
      WHEN (grid_accuracy > nb_p_accuracy) AND 
((grid_accuracy > accuracy) OR (accuracy IS NULL)) THEN 
grid_accuracy
      WHEN (nb_p_accuracy > accuracy)
    OR (accuracy IS NULL) THEN nb_p_accuracy
      WHEN accuracy IS NOT NULL THEN accuracy
    ELSE
    20
  END
    AS final_accuracy,
    GREATEST ( 0,
      CASE
        WHEN (grid_accuracy > nb_p_accuracy) AND 
((grid_accuracy > accuracy) OR (accuracy IS NULL)) THEN 
grid_confidence
        WHEN (nb_p_accuracy > accuracy)
      OR (accuracy IS NULL) THEN nb_p_confidence
        WHEN accuracy IS NULL THEN 0
        WHEN (grid_confidence > nb_p_confidence)
      AND (grid_confidence > 1) THEN grid_confidence
        WHEN (nb_p_confidence >= grid_confidence) OR 
((nb_p_confidence IS NOT NULL) AND ((grid_confidence IS 
NULL) OR (grid_confidence > 1))) THEN nb_p_confidence
      ELSE
      0
    END
      ) AS final_confidence
  FROM
    nb_p_table
  LEFT JOIN (
    SELECT
      point_text,
      grid_accuracy,
      grid_confidence
    FROM
      grid_table
    WHERE
      (grid_hits >= 100)
      OR (grid_confidence >= 0.1) )
  USING
    (point_text) ),
  remove_nulls AS (
  SELECT
    * EXCEPT(final_accuracy,
      final_confidence),
    CASE
      WHEN final_accuracy IS NULL THEN 20
      ELSE  final_accuracy
  END
    AS final_accuracy,
    CASE
      WHEN final_confidence IS NULL THEN 0
      ELSE final_confidence
  END
    AS final_confidence
  FROM
    final_quality_creation ),
  tranform_time_to_degrees AS (
  SELECT
    *,
    (hour_ts * 40) / (7200 * (110000 * 
COS(avg_lat_per_device_per_day))) AS lng_time,
    (hour_ts * 40) / (7200 * 110000) AS lat_time,
  FROM (
    SELECT
      *,
      AVG(latitude) OVER (PARTITION BY device_id, 
local_date) AS avg_lat_per_device_per_day,
      AVG(longitude) OVER (PARTITION BY device_id, 
local_date) AS avg_long_per_device_per_day
    FROM
      remove_nulls) ),
  shift_time AS (
  SELECT
    * EXCEPT(lng_time,
      lat_time,
      avg_lng_time,
      avg_lat_time,
      avg_lat_per_device_per_day),
    ((lng_time - avg_lng_time) + 
avg_long_per_device_per_day) AS lng_time,
    ((lat_time - avg_lat_time) + 
avg_lat_per_device_per_day) AS lat_time
  FROM (
    SELECT
      *,
      AVG(lng_time) OVER (PARTITION BY device_id, 
local_date) avg_lng_time,
      AVG(lat_time) OVER (PARTITION BY device_id, 
local_date) avg_lat_time,
      COUNT(device_id) OVER (PARTITION BY device_id, 
local_date) geohit_count
    FROM
      tranform_time_to_degrees ) )
      ,
  clustering_prep AS (
  SELECT
    *
  FROM (
    SELECT
      *,
      ST_GEOGPOINT(lng_time,
        latitude) AS lng_time_lat,
      ST_GEOGPOINT(longitude,
        lat_time) AS lng_lat_time,
      ROW_NUMBER() OVER () AS row_id
    FROM
      shift_time 
    WHERE geohit_count >= 9) ),
  db_clustering AS (
  SELECT
    *,
    CAST(CONCAT(cluster_lat_lngtime,cluster_lattime_lng) 
AS INTEGER) AS cluster_id
  FROM (
    SELECT
      timezone,
      device_id,
      device_os,
      row_id,
      local_date,
      local_hour,
      hour_ts,
      ST_GEOGPOINT(longitude,
        latitude) lat_long_point,
      latitude,
      longitude,
      final_accuracy,
      final_confidence,
      publisher_id,
      n_hits,
      country,
      ST_CLUSTERDBSCAN(lng_time_lat,
        45,
        3) OVER (PARTITION BY device_id, local_date) AS 
cluster_lat_lngtime,
      ST_CLUSTERDBSCAN(lng_lat_time,
        45,
        3) OVER (PARTITION BY device_id, local_date) AS 
cluster_lattime_lng,
    FROM
      clustering_prep
    ORDER BY
      row_id ) ),
  format_output AS (
  SELECT
    *,
    PERCENTILE_DISC(latitude,
      0.5) OVER(PARTITION BY device_id,local_date, 
cluster_id ) AS median_lat,
    PERCENTILE_DISC(longitude,
      0.5) OVER(PARTITION BY device_id, local_date, 
cluster_id ) AS median_lng,
    MAX(hour_ts) OVER (PARTITION BY device_id, 
local_date, cluster_id) AS max_hour_ts,
    MIN(hour_ts) OVER (PARTITION BY device_id, 
local_date, cluster_id) AS min_hour_ts,
  FROM
    db_clustering )
    ,
 
limit_clusters_per_device AS (
  SELECT unnested.* from (select unnested 
  FROM (
    SELECT ARRAY_AGG(x limit 9) k
    FROM   format_output x
    GROUP BY device_id, local_date),
    UNNEST(k) unnested)
 ),
  get_visits AS (
  SELECT
    device_id,
    device_os,
    timezone,
    local_date,
    (max_hour_ts-min_hour_ts) AS duration,
    TIMESTAMP_SECONDS(min_hour_ts) AS visit_ts,
    AVG(final_accuracy) AS accuracy,
    AVG(final_confidence) confidence,
    cluster_id,
    EXTRACT(YEAR
    FROM
      local_date) local_year,
    ST_ASTEXT(ST_GEOGPOINT(median_lng,
        median_lat)) AS lat_long_visit_point,
    n_hits,
    country,
    ANY_VALUE(publisher_id) publisher_id
  FROM
    limit_clusters_per_device
  WHERE cluster_id is not null
  GROUP BY
    device_id,
    device_os,
    timezone,
    local_date,
    max_hour_ts,
    min_hour_ts,
    local_year,
    lat_long_visit_point,
    n_hits,
    country,
    cluster_id 
  ),

join_back_single_geohit_clusters as (
    SELECT * from get_visits
    UNION ALL 
    SELECT    
    device_id,
    device_os,
    timezone,
    local_date,
    0 AS duration,
    TIMESTAMP(sdk_ts) AS visit_ts,
    final_accuracy AS accuracy,
    final_confidence confidence,
    null as cluster_id,
    EXTRACT(YEAR
    FROM
      local_date) local_year,
    ST_ASTEXT(ST_GEOGPOINT(longitude,
        latitude)) AS lat_long_visit_point,
    n_hits,
    country,
    publisher_id
    from shift_time where geohit_count < 9

  )

  ,
  first as ( SELECT
          *,
        FROM
        drop_duplicates
 ),
second as (
      SELECT
        local_hour < 7 AS morning,
        local_hour >= 19 AS night,
        local_hour >= 11
        AND local_hour < 16 AS daytime,
        device_id,
        device_os,
        timezone,
        local_date,
        latitude,
        longitude,
      FROM first
          ),
 third as (
  SELECT
      device_id,
      device_os,
      timezone,
      local_date,
      CASE
        WHEN SUM( IF (morning, 1, 0)) OVER day_device > 0 
THEN IF (morning, latitude, NULL)
        WHEN SUM(
      IF
        (night,
          1,
          0)) OVER day_device > 0 THEN
    IF
      (night,
        latitude,
        NULL)
      ELSE
      NULL
    END
      AS nighttime_latitude,
      CASE
        WHEN SUM( IF (morning, 1, 0)) OVER day_device > 0 
THEN IF (morning, longitude, NULL)
        WHEN SUM(
      IF
        (night,
          1,
          0)) OVER day_device > 0 THEN
    IF
      (night,
        longitude,
        NULL)
      ELSE
      NULL
    END
      AS nighttime_longitude,
    IF
      ( (SUM(
          IF
            (daytime,
              1,
              0)) OVER day_device > 0),
      IF
        (daytime,
          latitude,
          NULL),
        NULL ) AS daytime_latitude,
    IF
      ( (SUM(
          IF
            (daytime,
              1,
              0)) OVER day_device > 0),
      IF
        (daytime,
          longitude,
          NULL),
        NULL ) AS daytime_longitude
    FROM second
    WINDOW
      day_device AS (
      PARTITION BY
        device_id,
        timezone,
        local_date )
    ),
 fourth as (SELECT
    device_id,
    device_os,
    timezone,
    local_date,
    ST_GEOGPOINT( APPROX_QUANTILES(nighttime_longitude, 
2) [
    OFFSET
      (1)],
      APPROX_QUANTILES(nighttime_latitude, 2) [
    OFFSET
      (1)] ) AS nighttime_location,
    ST_GEOGPOINT( APPROX_QUANTILES(daytime_longitude, 2) 
[
    OFFSET
      (1)],
      APPROX_QUANTILES(daytime_latitude, 2) [
    OFFSET
      (1)] ) AS daytime_location,
  FROM third
  GROUP BY
    device_id,
    device_os,
    timezone,
    local_date),
    
  join_results AS (
  SELECT
    *
  FROM
    join_back_single_geohit_clusters a
  LEFT JOIN
    fourth b
  USING
    (device_id,
    device_os,
      timezone,
      local_date) )
           ,
drop_duplicates_end AS (
  SELECT k.*
  FROM (
    SELECT ARRAY_AGG(x limit 1)[offset(0)] k
    FROM   join_results x
    GROUP BY visit_ts, device_id)
 )
SELECT
device_id, timezone, local_date, duration, visit_ts, accuracy, confidence, cluster_id, local_year, 
  EXTRACT(hour
  FROM
    visit_ts AT TIME ZONE timezone) local_hour, publisher_id, n_hits, country, 
  ST_GEOGFROMTEXT(lat_long_visit_point) 
lat_long_visit_point,device_os, nighttime_location, daytime_location 
  from drop_duplicates_end 