WITH
  log_query AS (
    SELECT 
      DATE("{{ ds }}") as provider_date, 
      local_date,
      count(*) as row_count
    FROM 
      `{{ params['project'] }}.{{ params['device_geohits_staging_dataset'] }}.{{ staging_table }}`
    GROUP BY local_date
  )
  select *
  from log_query
