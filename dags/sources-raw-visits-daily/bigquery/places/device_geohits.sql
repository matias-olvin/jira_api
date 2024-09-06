WITH
  drop_duplicates AS (
  SELECT k.*
  FROM (
    SELECT ARRAY_AGG(x limit 1)[offset(0)] k
    FROM 
      `{{ params['project'] }}.{{ params['raw_data_dataset'] }}.{{ dag_run.logical_date.strftime('%Y') }}` x
    GROUP BY sdk_ts, device_id, publisher_id)
  ),

  join_timezone AS (
  SELECT
    a.*,
    b.tzid as timezone
  FROM
    drop_duplicates a
  JOIN
  `{{ params['project'] }}.{{ params['geometries_dataset'] }}.{{ params['timezones_table'] }}` b
  --  `storage-prod-olvin-com.area_geometries.worldwide_timezones` b
  ON
    ST_INTERSECTS( ST_GEOGPOINT(a.longitude,
        a.latitude),
      b.geometry  ) 
  
  ),
      
column_transformation_2021 as (
  SELECT  
    CAST(event_id as string) event_id,
    event_ts,
    sdk_ts,
    EXTRACT(HOUR FROM sdk_ts AT TIME ZONE timezone) as local_hour,
    EXTRACT(DATE FROM sdk_ts AT TIME ZONE timezone) as local_date,
    country,
    region,
    latitude,
    longitude,
    accuracy,
    CAST(publisher_id as INTEGER) publisher_id,
    CASE
      WHEN LENGTH(device_id) = 36 THEN TO_BASE64(SHA1(CONCAT(REPLACE(device_id, "-", ""), '62Ou6@tYZ&')))  --Unencrypted HEX
      WHEN LENGTH(device_id) = 24 THEN TO_BASE64(SHA1(CONCAT(TO_HEX(FROM_BASE64(device_id)), '62Ou6@tYZ&')))  --Unencrypted BASE64
      WHEN LENGTH(device_id) = 28 THEN device_id  -- Encrypted BASE64
      ELSE CONCAT(device_id, '_')  
    END AS device_id,
    device_os,
    device_os_version,
    timezone,
    ST_GEOGPOINT(longitude, latitude) as long_lat_point
  FROM 
    join_timezone
)

SELECT
 *
FROM
column_transformation_2021

