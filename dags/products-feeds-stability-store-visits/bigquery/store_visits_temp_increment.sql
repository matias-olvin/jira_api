CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}`
PARTITION BY week_starting
CLUSTER BY store_id
AS
with get_visits as (
  SELECT *
  FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplacedailyvisitsraw_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
  -- Include local_date filter to reduce cost of the query
  WHERE 
    local_date >= DATE_TRUNC('{{ ds }}', MONTH)
    AND local_date < DATE_TRUNC(DATE_ADD('{{ ds }}', INTERVAL 2 MONTH), MONTH)
),
min_date as (
  select
    min(local_date) as min_local_date
  from get_visits
),
-- explode the visits
daily_visits as (
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
    CAST(visits as INT64) visits,
    fk_sgplaces,
    DATE_TRUNC(DATE_ADD(local_date, INTERVAL row_number DAY), WEEK(SUNDAY)) as week_start
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM get_visits
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array as another column
  ORDER BY
    local_date,
    fk_sgplaces,
    row_number
),
adding_visits_columns as (
  select
    a.fk_sgplaces,
    total_visits,
    week_starting,
    week_ending,
    CASE 
      WHEN min_local_date > week_starting
        THEN TO_JSON_STRING(ARRAY_CONCAT(ARRAY(SELECT 0 FROM UNNEST(GENERATE_ARRAY(1,DATE_DIFF(min_local_date, week_starting, DAY)))), ARRAY_AGG(visits ORDER BY local_date)))
      ELSE
        CASE 
          WHEN max_local_date < week_ending
            THEN TO_JSON_STRING( ARRAY_CONCAT(ARRAY_AGG(visits ORDER BY local_date),ARRAY(SELECT 0 FROM UNNEST(GENERATE_ARRAY(1,DATE_DIFF(week_ending, max_local_date, DAY))))))
          ELSE TO_JSON_STRING(ARRAY_AGG(visits ORDER BY local_date))
        END
      END AS daily_visits
  FROM (
    select
      fk_sgplaces,
      sum(visits) as total_visits,
      week_start as week_starting,
      DATE_ADD(week_start, INTERVAL 6 DAY) as week_ending,
      min(local_date) as min_local_date,
      max(local_date) as max_local_date
    from daily_visits
    group by
        fk_sgplaces,
        week_start
  ) a
  inner join
    daily_visits
    on a.fk_sgplaces = daily_visits.fk_sgplaces and week_start = week_starting
  group by
    fk_sgplaces,
    total_visits,
    week_starting,
    week_ending,
    min_local_date,
    max_local_date
),
adding_places_columns as (
  select
    fk_sgplaces,
    name,
    brand,
    street_address,
    city,
    state,
    zip_code,
    week_starting,
    week_ending,
    total_visits,
    daily_visits,
  from adding_visits_columns
  inner join (
    select
      pid as fk_sgplaces,
      name,
      fk_sgbrands,
      street_address,
      city,
      region as state,
      postal_code as zip_code
    from
      `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
  ) using (fk_sgplaces)
  inner join (
    select
      pid as fk_sgbrands,
      name as brand
    from
      `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgbrandraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
  ) using (fk_sgbrands)
),
-- Include hourly visits for each weekday (we have to create an hourly table with past visits, not only with predictions)
hourly_visits AS (
  SELECT
    fk_sgplaces,
    DATE_TRUNC(local_date, WEEK) AS week_starting,
    DATE_DIFF(local_date, DATE_TRUNC(local_date, WEEK), DAY) AS day_of_week,
    visits
  FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['hourly_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
    -- Include local_date filter to reduce cost of the query
  WHERE
    local_date >= DATE_TRUNC('{{ ds }}', MONTH)
),
hourly_columns AS (
  SELECT
    fk_sgplaces,
    week_starting,
    IFNULL(ARRAY_TO_STRING(array_agg(CASE WHEN day_of_week = 0 THEN visits END IGNORE NULLS), ','),
                                                '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]') AS sunday_hourly,
    IFNULL(ARRAY_TO_STRING(array_agg(CASE WHEN day_of_week = 1 THEN visits END IGNORE NULLS), ','),
                                                '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]') AS monday_hourly,
    IFNULL(ARRAY_TO_STRING(array_agg(CASE WHEN day_of_week = 2 THEN visits END IGNORE NULLS), ','),
                                                '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]') AS tuesday_hourly,
    IFNULL(ARRAY_TO_STRING(array_agg(CASE WHEN day_of_week = 3 THEN visits END IGNORE NULLS), ','),
                                                '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]') AS wednesday_hourly,
    IFNULL(ARRAY_TO_STRING(array_agg(CASE WHEN day_of_week = 4 THEN visits END IGNORE NULLS), ','),
                                                '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]') AS thursday_hourly,
    IFNULL(ARRAY_TO_STRING(array_agg(CASE WHEN day_of_week = 5 THEN visits END IGNORE NULLS), ','),
                                                '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]') AS friday_hourly,
    IFNULL(ARRAY_TO_STRING(array_agg(CASE WHEN day_of_week = 6 THEN visits END IGNORE NULLS), ','),
                                                '[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]') AS saturday_hourly
  FROM hourly_visits
  GROUP BY fk_sgplaces, week_starting
),
adding_hourly_columns as (
  select
    fk_sgplaces as store_id,
    name,
    brand,
    street_address,
    city,
    state,
    zip_code,
    week_starting,
    week_ending,
    total_visits,
    daily_visits,
    sunday_hourly,
    monday_hourly,
    tuesday_hourly,
    wednesday_hourly,
    thursday_hourly,
    friday_hourly,
    saturday_hourly
  from adding_places_columns
  inner join hourly_columns
  using(fk_sgplaces, week_starting)
)

select *
from adding_hourly_columns
inner join(
    SELECT fk_sgplaces as store_id
    FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceactivity_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}` as brands
    -- `storage-prod-olvin-com.postgres_final.SGPlaceActivity`
    WHERE activity = 'active'
)
USING(store_id)
