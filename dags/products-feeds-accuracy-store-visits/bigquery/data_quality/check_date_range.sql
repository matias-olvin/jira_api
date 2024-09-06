SELECT COUNT(*) AS _count
FROM 
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}` 
WHERE
  DATE_DIFF(week_ending, week_starting, DAY) < 0
  OR DATE_DIFF(week_ending, week_starting, DAY) > 6
  OR EXTRACT(DAYOFWEEK FROM week_starting) != 1
