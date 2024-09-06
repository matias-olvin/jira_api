create or replace table
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['SGPlacePatternVisitsRaw_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.SGPlacePatternVisitsRaw`
    partition by local_date
    cluster by fk_sgplaces
as

WITH
-- DAILY
-- explode the visits
daily_visits as (
  SELECT
    CAST(visits as INT64) visits,
    fk_sgplaces,
    EXTRACT(DAYOFWEEK FROM DATE_ADD(local_date, INTERVAL row_number DAY)) - 1 as day_of_week,
    local_date AS month
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['SGPlaceDailyVisitsRaw_table'] }}`
--    `storage-prod-olvin-com.postgres_final.SGPlaceDailyVisitsRaw`
    WHERE local_date = '{{ ds }}'
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array as another column
),

filtered_daily_vars AS (
  SELECT fk_sgplaces,
        month AS local_date,
        day_of_week,
        AVG(visits) AS avg_visits
  FROM daily_visits
  GROUP BY fk_sgplaces,
            month,
            day_of_week
),

-- HOURLY
-- explode the visits
hourly_visits as (
  SELECT
    CAST(visits as INT64) visits,
    fk_sgplaces,
    hour + 24*(EXTRACT(DAYOFWEEK FROM local_date) - 1) AS hour_of_week,
    DATE_TRUNC(local_date, MONTH) AS month
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['SGPlaceHourlyAllVisitsRaw_table'] }}`
    -- `storage-prod-olvin-com.postgres_final.SGPlaceHourlyAllVisitsRaw`
    WHERE local_date < DATE_ADD('{{ ds }}', INTERVAL 1 MONTH) -- 1st day of following month
      AND local_date >= '{{ ds }}'
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS hour -- Get the position in the array as another column
),

filtered_hourly_vars AS (
  SELECT fk_sgplaces,
        month AS local_date,
        hour_of_week,
        AVG(visits) AS avg_visits
  FROM hourly_visits
  GROUP BY fk_sgplaces,
            month,
            hour_of_week
)

SELECT *
FROM (
  SELECT fk_sgplaces, local_date, TO_JSON_STRING(ARRAY_AGG(avg_visits ORDER BY day_of_week ASC)) as day_of_week
  FROM filtered_daily_vars
  GROUP BY fk_sgplaces, local_date
)
INNER JOIN (
  SELECT fk_sgplaces, local_date, TO_JSON_STRING(ARRAY_AGG(avg_visits ORDER BY hour_of_week ASC)) as time_of_day
  FROM filtered_hourly_vars
  GROUP BY fk_sgplaces, local_date
)
USING(fk_sgplaces, local_date)
UNION ALL (
SELECT *
FROM
    `{{ params['storage_prod'] }}.{{ params['places_postgres_batch_dataset'] }}.{{  params['SGPlacePatternVisitsRaw_table'] }}`
WHERE local_date <> '{{ ds }}'
)