ASSERT (
  WITH weekly_visits AS (
    SELECT
      week_starting
      , SUM(total_visits) AS total_visits 
    FROM 
      `{{ var.value.prod_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}` 
    GROUP BY week_starting
  )
  , weekly_avg AS (
    SELECT AVG(total_visits) AS average
    FROM weekly_visits
  )

  SELECT COUNT(*)
  FROM weekly_visits, weekly_avg
  WHERE 
    total_visits < average * 0.10 OR
    total_visits > average * 1.60
) = 0