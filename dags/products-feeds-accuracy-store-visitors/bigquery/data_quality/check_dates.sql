SELECT COUNT(*) AS _count
FROM 
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visitors_temp_table'] }}` 
WHERE 
  EXTRACT(DAY FROM month_starting) != 1
  OR month_ending != LAST_DAY(month_starting)
