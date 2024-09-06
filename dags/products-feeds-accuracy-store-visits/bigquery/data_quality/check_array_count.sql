SELECT COUNT(*) as _count
FROM (
  SELECT
    store_id
    , ABS(DATE_DIFF(week_ending, week_starting, DAY)) AS days_count
    , MAX(day_number) AS daily_visits_count
  FROM 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}`
  CROSS JOIN 
    UNNEST(JSON_EXTRACT_ARRAY(daily_visits)) AS visits
    WITH OFFSET AS day_number
  GROUP BY 
    store_id
    , week_starting
    , week_ending
  HAVING days_count != daily_visits_count
)
