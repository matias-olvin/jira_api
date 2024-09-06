UPDATE 
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}` 
SET sunday_hourly = '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]'
WHERE sunday_hourly IS NULL
