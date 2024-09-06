ASSERT (
  WITH monthly_visits AS (
    SELECT
      month_starting
      , SUM(total_visits) AS total_visits 
    FROM 
      `{{ var.value.prod_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_trend_temp_table'] }}` 
    GROUP BY month_starting
  )
  , monthly_avg AS (
    SELECT AVG(total_visits) AS average
    FROM monthly_visits
  )

  SELECT COUNT(*)
  FROM monthly_visits, monthly_avg
  WHERE 
    total_visits < average * 0.10 OR
    total_visits > average * 1.60
) = 0