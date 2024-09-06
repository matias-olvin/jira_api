SELECT COUNT(*) AS _count
FROM
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}` 
WHERE friday_hourly IS NULL
