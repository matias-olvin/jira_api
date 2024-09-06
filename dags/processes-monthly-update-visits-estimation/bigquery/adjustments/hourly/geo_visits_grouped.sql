  --HOURLY geo visits grouped
  create or replace table
  `{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_hourly_geo_visits_grouped'] }}`
    -- `storage-dev-olvin-com.visits_estimation.adjustments_hourly_geo_visits_grouped`
    as
WITH
  get_timezone AS (
  SELECT
    fk_sgplaces,
    local_date,
    hour_ts AS ds,
    visits_observed AS visits,
    timezone
  FROM
    `{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['visits_aggregated_table'] }}`
    -- `storage-prod-olvin-com.poi_aggregated_visits.visits_observed`
  LEFT JOIN
    `{{  var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}`
    -- `storage-dev-olvin-com.sg_places.places_dynamic`
  ON
    pid = fk_sgplaces ),
  get_groups AS (
  SELECT
    fk_sgplaces,
    local_date,
    tier_id_olvin,
    EXTRACT(HOUR
    FROM
      TIMESTAMP_SECONDS(ds) AT TIME ZONE timezone) AS local_hour,
    EXTRACT(dayofweek
    FROM
      EXTRACT(date
      FROM
        TIMESTAMP_SECONDS(ds) AT TIME ZONE timezone)) AS day_of_week,
    24 * (EXTRACT(dayofweek
      FROM
        EXTRACT(date
        FROM
          TIMESTAMP_SECONDS(ds) AT TIME ZONE timezone)) - 1) + EXTRACT(HOUR
    FROM
      TIMESTAMP_SECONDS(ds) AT TIME ZONE timezone) AS hour_week,
    visits
  FROM
    get_timezone
  INNER JOIN
    `{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.poi_class`
  USING
    (fk_sgplaces)
  WHERE
    fk_sgplaces NOT LIKE "%_gt" ),
  dummy_dates AS (
  SELECT
    *,
    CASE
      WHEN day_of_week = 1 THEN DATE("2023-02-19")
      WHEN day_of_week = 2 THEN DATE("2023-02-20")
      WHEN day_of_week = 3 THEN DATE("2023-02-21")
      WHEN day_of_week = 4 THEN DATE("2023-02-22")
      WHEN day_of_week = 5 THEN DATE("2023-02-23")
      WHEN day_of_week = 6 THEN DATE("2023-02-24")
      WHEN day_of_week = 7 THEN DATE("2023-02-25")
  END
    AS dummy_date
  FROM
    get_groups ),
  hourly_sum_visits AS (
  SELECT
    tier_id_olvin,
    dummy_date,
    hour_week,
    SUM(visits) hourly_visits
  FROM
    dummy_dates
  GROUP BY
    1,
    2,
    3 ),
  calc_daily_visits AS (
  SELECT
    tier_id_olvin,
    dummy_date,
    SUM(hourly_visits) daily_visits
  FROM
    hourly_sum_visits
  GROUP BY
    1,
    2 ),
  scale_hourly_visits_by_day AS (
  SELECT
    tier_id_olvin,
    hour_week,
    daily_visits,
    hourly_visits,
    IFNULL(hourly_visits/
    IF
      (daily_visits = 0, NULL,daily_visits), 0) hourly_visits_scaled
  FROM
    hourly_sum_visits
  INNER JOIN
    calc_daily_visits
  USING
    (tier_id_olvin,
      dummy_date) ),
  add_day_of_week as (
    SELECT
        *,
        CASE
          WHEN hour_week < 24 THEN 1
          WHEN hour_week >= 24
        AND hour_week < 48 THEN 2
          WHEN hour_week >= 48 AND hour_week < 72 THEN 3
          WHEN hour_week >= 72
        AND hour_week < 96 THEN 4
          WHEN hour_week >= 96 AND hour_week < 120 THEN 5
          WHEN hour_week >= 120
        AND hour_week < 144 THEN 6
          WHEN hour_week >= 144 THEN 7
      END
        AS day_of_week,
      FROM
        scale_hourly_visits_by_day

  ),

  get_non_zero_elements as (
select tier_id_olvin, day_of_week, daily_visits, 
LEAD(daily_visits, 1) OVER (partition by tier_id_olvin order by day_of_week) as lag_1, 
LAG(daily_visits, 1) OVER (partition by tier_id_olvin order by day_of_week) as lead_1 , 
LEAD(daily_visits, 2) OVER (partition by tier_id_olvin order by day_of_week) as lag_2, 
LAG(daily_visits, 2) OVER (partition by tier_id_olvin order by day_of_week) as lead_2,
  LEAD(daily_visits, 3) OVER (partition by tier_id_olvin order by day_of_week) as lag_3, 
  LAG(daily_visits, 3) OVER (partition by tier_id_olvin order by day_of_week) as lead_3 ,
   LEAD(daily_visits, 4) OVER (partition by tier_id_olvin order by day_of_week) as lag_4, 
   LAG(daily_visits, 4) OVER (partition by tier_id_olvin order by day_of_week) as lead_4 ,
from add_day_of_week 
where day_of_week < 6
group by tier_id_olvin, day_of_week, daily_visits 
 order by day_of_week
),

  fill_zero_days AS (
  select tier_id_olvin, day_of_week, daily_visits, hour_week, hourly_visits_scaled, CASE 
WHEN daily_visits = 0 and lag_1 >0 THEN LEAD(hourly_visits_scaled, 24) OVER (partition by tier_id_olvin ORDER BY hour_week)
WHEN daily_visits = 0 and lead_1 >0 THEN LAG(hourly_visits_scaled, 24) OVER (partition by tier_id_olvin ORDER BY hour_week) 
WHEN daily_visits = 0 and lag_2 >0 THEN LEAD(hourly_visits_scaled, 48) OVER (partition by tier_id_olvin ORDER BY hour_week )
WHEN daily_visits = 0 and lead_2 >0 THEN LAG(hourly_visits_scaled, 48) OVER (partition by tier_id_olvin ORDER BY hour_week) 
WHEN daily_visits = 0 and lag_3 >0 THEN LEAD(hourly_visits_scaled, 72) OVER (partition by tier_id_olvin ORDER BY hour_week )
WHEN daily_visits = 0 and lead_3 >0 THEN LAG(hourly_visits_scaled, 72) OVER (partition by tier_id_olvin ORDER BY hour_week) 
WHEN daily_visits = 0 and lag_4 >0 THEN LEAD(hourly_visits_scaled, 96) OVER (partition by tier_id_olvin ORDER BY hour_week )
WHEN daily_visits = 0 and lead_4 >0 THEN LAG(hourly_visits_scaled, 96) OVER (partition by tier_id_olvin ORDER BY hour_week )
Else hourly_visits_scaled 

end as hourly_visits_scaled_tweak 
from 
add_day_of_week left join get_non_zero_elements
using (tier_id_olvin, day_of_week, daily_visits)
order by hour_week
  ),
  join_back_fk_sgplaces AS (
  SELECT
    * EXCEPT(hourly_visits_scaled), hourly_visits_scaled_tweak as hourly_visits_scaled
  FROM (
    SELECT
      DISTINCT fk_sgplaces,
      tier_id_olvin
    FROM
      get_groups) t1
  LEFT JOIN
    fill_zero_days t2
  USING
    (tier_id_olvin) )
SELECT
  *
FROM
join_back_fk_sgplaces
