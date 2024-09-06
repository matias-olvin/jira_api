create or replace table `{{ params['project'] }}.{{ params['raw_data_dataset'] }}.{{ execution_date.strftime('%Y') }}`
partition by date_trunc(sdk_ts,DAY)
as

SELECT 
  TIMESTAMP(event_ts) AS event_ts,
  TIMESTAMP(sdk_ts) AS sdk_ts,
  CAST(device_id AS STRING) AS device_id,
  CAST(device_os AS STRING) AS device_os,
  CAST(device_os_version AS STRING) AS device_os_version,
  CAST(country AS STRING) AS country,
  CAST(region AS STRING) AS region,
  CAST(latitude AS FLOAT64) AS latitude,
  CAST(longitude AS FLOAT64) AS longitude,
  CAST(accuracy AS FLOAT64) AS accuracy,
  CAST(event_id AS STRING) AS event_id,
  CAST(publisher_id AS FLOAT64) AS publisher_id
FROM
  `{{ params['project'] }}.{{ params['raw_data_dataset'] }}.{{ execution_date.strftime('%Y') }}_mobilewalla`
UNION ALL
SELECT
  TIMESTAMP(event_ts) AS event_ts,
  TIMESTAMP(sdk_ts) AS sdk_ts,
  CAST(device_id AS STRING) AS device_id,
  CAST(device_os AS STRING) AS device_os,
  CAST(device_os_version AS STRING) AS device_os_version,
  CAST(country AS STRING) AS country,
  CAST(region AS STRING) AS region,
  CAST(latitude AS FLOAT64) AS latitude,
  CAST(longitude AS FLOAT64) AS longitude,
  CAST(accuracy AS FLOAT64) AS accuracy,
  CAST(event_id AS STRING) AS event_id,
  CAST(publisher_id AS FLOAT64) AS publisher_id 
FROM 
  `{{ params['project'] }}.{{ params['raw_data_dataset'] }}.{{ execution_date.strftime('%Y') }}_non_mobilewalla`
