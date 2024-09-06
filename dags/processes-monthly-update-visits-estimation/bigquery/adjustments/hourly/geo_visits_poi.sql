
create
or replace table 
`{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_hourly_geo_visits_poi'] }}`
-- `storage-dev-olvin-com.visits_estimation.adjustments_hourly_geo_visits_poi` 
as 
WITH get_timezone AS (
  SELECT
    fk_sgplaces,
    local_date,
    hour_ts as ds,
    visits_observed AS visits,
    timezone
  FROM
    `{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['visits_aggregated_table'] }}`
    -- `storage-prod-olvin-com.poi_aggregated_visits.visits_observed`
    LEFT JOIN 
    `{{  var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}`
    -- `storage-dev-olvin-com.sg_places.places_dynamic` 
    ON pid = fk_sgplaces
),
get_hour_week AS (
  SELECT
    fk_sgplaces,
    local_date,
    EXTRACT(
      HOUR
      FROM
        TIMESTAMP_SECONDs(ds) AT TIME ZONE timezone
    ) AS local_hour,
    EXTRACT(
      dayofweek
      FROM
        EXTRACT(
          date
          FROM
            TIMESTAMP_SECONDs(ds) AT TIME ZONE timezone
        )
    ) AS day_of_week,
    24 * (
      EXTRACT(
        dayofweek
        FROM
          EXTRACT(
            date
            FROM
              TIMESTAMP_SECONDs(ds) AT TIME ZONE timezone
          )
      ) - 1
    ) + EXTRACT(
      HOUR
      FROM
        TIMESTAMP_SECONDs(ds) AT TIME ZONE timezone
    ) AS hour_week,
    visits
  FROM
    get_timezone
),
hourly_sum_visits AS (
  SELECT
    fk_sgplaces,
    hour_week,
    day_of_week,
    SUM(visits) hourly_visits
  FROM
    get_hour_week
  group by
    1,
    2,
    3
),
hour_week_template as (
  SELECT *,  CASE
      WHEN hour_week < 24 THEN 1
      WHEN hour_week >= 24
      and hour_week < 48 THEN 2
      WHEN hour_week >= 48
      and hour_week < 72 THEN 3
      WHEN hour_week >= 72
      and hour_week < 96 THEN 4
      WHEN hour_week >= 96
      and hour_week < 120 THEN 5
      WHEN hour_week >= 120
      and hour_week < 144 THEN 6
      WHEN hour_week >= 144 THEN 7
    END as day_of_week
    FROM 
    (select distinct fk_sgplaces from hourly_sum_visits),(select hour_week FROM UNNEST(GENERATE_ARRAY(0, 167)) AS hour_week) 
),
fix_missing_hour_week as (
select fk_sgplaces, hour_week, day_of_week, COALESCE(t2.hourly_visits, 0) as hourly_visits from hour_week_template 
left join 
hourly_sum_visits t2
using (fk_sgplaces, hour_week, day_of_week)
),
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
    END AS dummy_date
  FROM
    fix_missing_hour_week
),
smoothing_mva AS (
  SELECT
    *
  EXCEPT
(hourly_visits_smoothed),
    IF(hourly_visits = 0, 0, hourly_visits_smoothed) hourly_visits_smoothed
  FROM
    (
      SELECT
        fk_sgplaces,
        hour_week,
        hourly_visits,
        dummy_date,
        AVG(hourly_visits) OVER(
          partition by fk_sgplaces
          ORDER BY
            hour_week ROWS BETWEEN 2 PRECEDING
            AND 2 FOLLOWING
        ) AS hourly_visits_smoothed
      FROM
        dummy_dates
    )
),
calc_daily_visits as (
  SELECT
    fk_sgplaces,
    dummy_date,
    sum(hourly_visits) daily_visits
  from
    dummy_dates
  group by
    1,
    2
),
scale_hourly_visits_by_day AS (
  SELECT
    fk_sgplaces,
    hour_week,
    daily_visits,
    hourly_visits,
    hourly_visits_smoothed,
    IFNULL(
      hourly_visits_smoothed / IF(daily_visits = 0, null, daily_visits),
      0
    ) hourly_visits_scaled
  FROM
    smoothing_mva
    inner join calc_daily_visits using (fk_sgplaces, dummy_date)
),
calc_ratio_visits_detected as (
  select
    *
  from
    scale_hourly_visits_by_day
    inner join (
      select
        fk_sgplaces,
        ifnull(
          count(if(visits > 0, 1, null)) / nullif(count(if(max_visit_week_hour > 0, 1, null)), 0),
          0
        ) ratio_visits_detected,
      from
        (
          select
            *,
            max(visits) over (partition by fk_sgplaces, hour_week) as max_visit_week_hour
          from
            (
              select
                fk_sgplaces,
                local_date,
                local_hour,
                hour_week,
                ifnull(visits, 0) visits
              from
                (
                  select
                    distinct fk_sgplaces
                  from
                    get_hour_week
                ),
                (
                  select
                    distinct local_date,
                    local_hour,
                    hour_week
                  from
                    get_hour_week
                )
                left join get_hour_week using (fk_sgplaces, local_date, local_hour, hour_week)
            )
        )
      group by
        1
    ) using (fk_sgplaces)
),
check_rows as (
  select *, count(*) over (partition by fk_sgplaces) row_count from calc_ratio_visits_detected 
)

select
  * EXCEPT(row_count)
from
  check_rows
WHERE IF(
(row_count = 168),
TRUE,
ERROR(FORMAT("Not all fk_sgplaces have 168 hours of the week"))
) 
