-- Description
-- This script is used to generate the hourly output for the visits estimation
-- The first part (BEGIN CREATE TEMP TABLE week_array as) hardcodes the weekday opening hours because some of them were wrongly set to closed.

BEGIN CREATE TEMP TABLE places_new_hours as

WITH original as (
    select pid as fk_sgplaces,  open_hours
    from
      -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
      `{{ var.value.prod_project }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
),
find_non_empty_hours as (
  SELECT fk_sgplaces, open_hours, CASE 
  WHEN open_hours NOT LIKE '%"Wed": []%' THEN REPLACE(JSON_EXTRACT(open_hours, '$.Wed'), ',', ', ')
  WHEN open_hours NOT LIKE '%"Tue": []%' THEN REPLACE(JSON_EXTRACT(open_hours, '$.Tue'), ',', ', ')
  WHEN open_hours NOT LIKE '%"Thu": []%' THEN REPLACE(JSON_EXTRACT(open_hours, '$.Thu'), ',', ', ')
  WHEN open_hours NOT LIKE '%"Mon": []%' THEN REPLACE(JSON_EXTRACT(open_hours, '$.Mon'), ',', ', ')
  WHEN open_hours NOT LIKE '%"Fri": []%' THEN REPLACE(JSON_EXTRACT(open_hours, '$.Fri'), ',', ', ')
  Else null
  END AS generic_opening_hours
from original
)

select fk_sgplaces ,
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(open_hours,
                '"Mon": []', CONCAT('"Mon": ',generic_opening_hours )
                ),
              '"Tue": []', CONCAT('"Tue": ',generic_opening_hours )
              ),
            '"Wed": []', CONCAT('"Wed": ',generic_opening_hours )
            ),
          '"Thu": []', CONCAT('"Thu": ',generic_opening_hours )
          ),
        '"Fri": []', CONCAT('"Fri": ',generic_opening_hours )
        )
as open_hours
FROM find_non_empty_hours 
;end;

BEGIN CREATE TEMP TABLE week_array_table_temp as

WITH
fill_empty_open_hours as (
  SELECT
    * EXCEPT(open_hours),
    CASE
      WHEN open_hours is null THEN '{ "Mon": [["9:00", "21:00"]], "Tue": [["9:00", "21:00"]], "Wed": [["9:00", "21:00"]], "Thu": [["9:00", "21:00"]], "Fri": [["9:00", "21:00"]], "Sat": [["10:00", "21:00"]], "Sun": [["11:00", "19:00"]] }'
      ELSE replace(open_hours, r'[]', '["0:00","0:00"]')
    END AS open_hours
  FROM
    places_new_hours
),

split_open_hours_by_day as (
  SELECT
    open_hours,
    fk_sgplaces,
    ARRAY_TO_STRING(SPLIT(REGEXP_REPLACE(REGEXP_EXTRACT(open_hours, "{.+Tue"),r'\], \[', ']"Mon"['), "Tue"),"") as Mon_temp,
    ARRAY_TO_STRING(SPLIT(REGEXP_REPLACE(REGEXP_EXTRACT(open_hours, "Tue.+Wed") ,r'\], \[', ']"Tue"['), "Wed"),"") as Tue_temp,
    ARRAY_TO_STRING(SPLIT(REGEXP_REPLACE(REGEXP_EXTRACT(open_hours, "Wed.+Thu" ),r'\], \[', ']"Wed"['), "Thu"),"") as Wed_temp,
    ARRAY_TO_STRING(SPLIT(REGEXP_REPLACE(REGEXP_EXTRACT(open_hours, "Thu.+Fri") ,r'\], \[', ']"Thu"['), "Fri"),"") as Thu_temp,
    ARRAY_TO_STRING(SPLIT(REGEXP_REPLACE(REGEXP_EXTRACT(open_hours, "Fri.+Sat" ),r'\], \[', ']"Fri"['), "Sat"),"") as Fri_temp,
    ARRAY_TO_STRING(SPLIT(REGEXP_REPLACE(REGEXP_EXTRACT(open_hours, "Sat.+Sun") ,r'\], \[', ']"Sat"['), "Sun"),"") as Sat_temp,
    ARRAY_TO_STRING(SPLIT(REGEXP_REPLACE(REGEXP_EXTRACT(open_hours, "Sun.+" ),r'\], \[', ']"Sun"['), "*"),"") as Sun_temp
  FROM
    fill_empty_open_hours
),
concat_output as (
  SELECT
    fk_sgplaces,
    open_hours,
    concat(Mon_temp, Tue_temp, Wed_temp, Thu_temp, Fri_temp, Sat_temp, Sun_temp) as transformed_open_hours
  FROM
    split_open_hours_by_day
),
split_opening_hours_dict as (
  SELECT
      *,
      REGEXP_EXTRACT_ALL(transformed_open_hours, r'"(\w+)"') as day_of_week,
      (SELECT ARRAY_AGG(CAST(id as INT64)) FROM UNNEST(REGEXP_EXTRACT_ALL(transformed_open_hours, r'\["(\d+):\d+')) id)  as opening_hour,
      (SELECT ARRAY_AGG(CAST(id as INT64)) FROM UNNEST(REGEXP_EXTRACT_ALL(transformed_open_hours, r'"(\d+):\d+"\]'))id)  as closing_hour
  FROM
    concat_output
),
unnest_array as (
  SELECT
    fk_sgplaces ,
    opening_hour,
    closing_hour,
    `storage-prod-olvin-com.sg_places.map_week_array` (
      day_of_week,
      [
        ("Sun", 0),
        ("Mon", 1),
        ("Tue", 2),
        ("Wed", 3),
        ("Thu", 4),
        ("Fri", 5),
        ("Sat", 6)
      ], 0
    ) AS day_of_week_int
  FROM  (
    SELECT
      fk_sgplaces,
      opening_hour,
      closing_hour,
      day_of_week
    FROM
      split_opening_hours_dict,
      unnest(day_of_week) day_of_week with offset pos1,
      unnest(opening_hour) opening_hour with offset pos2,
      unnest(closing_hour) closing_hour with offset pos3
    WHERE pos1 = pos2
    AND pos2 = pos3
  )
),
places_opening_hours_convert_to_week AS (
  SELECT
    fk_sgplaces,
    day_of_week_int * 24 + opening_hour as opening_week_hour,
    day_of_week_int * 24 + closing_hour as closing_week_hour,
    closing_hour - opening_hour as open_length
    FROM
      unnest_array
),
transform_hours AS (
  SELECT
    fk_sgplaces,
    opening_week_hour AS before_opening,
    closing_week_hour-opening_week_hour AS opening,
    168-closing_week_hour AS after_opening
  FROM
    places_opening_hours_convert_to_week
),
concat_hours AS (
  SELECT
    *,
    concat(
      '[0*', CAST(before_opening as STRING),
      ',1*', CAST(opening as STRING),
      ',0*',  CAST(after_opening as STRING), ']'
    ) AS concat_opening
  FROM
    transform_hours
),
create_week_array AS (
  SELECT
    * EXCEPT(before_opening, opening, after_opening, concat_opening),
    `storage-prod-olvin-com.sg_places.expand_list_week_array`(concat_opening) AS week_array
  FROM
    concat_hours
),
places_opening_hours_week_array AS (
  SELECT
    fk_sgplaces,
    ARRAY_AGG(x ORDER BY day_of_week_int) AS week_array
  FROM (
    SELECT
      fk_sgplaces,
      day_of_week_int,
      LEAST(SUM(CAST(x as INT64)),1) as x
    FROM
      create_week_array,
      UNNEST(week_array) as x with OFFSET day_of_week_int
    GROUP BY
      fk_sgplaces,
      day_of_week_int
  )
  group by fk_sgplaces
)
SELECT fk_sgplaces as pid, week_array
FROM

places_opening_hours_week_array 
;end;






-- Second Part: Creating hourly visits for each POI

CREATE
OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_output_hourly_table'] }}` 
--`storage-dev-olvin-com.visits_estimation.adjustments_output_hourly` 
partition by local_date AS 
WITH combine_inputs as (
  SELECT
    t2.fk_sgplaces,
    t2.hour_week,
    t2.tier_id_olvin,
    t2.hourly_visits_scaled as group_hourly_visits,
    t3.hourly_visits_scaled as poi_hourly_visits,
    t3.ratio_visits_detected
  FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_hourly_geo_visits_grouped'] }}` t2 
    -- `storage-dev-olvin-com.visits_estimation.adjustments_hourly_geo_visits_grouped`t2
    inner join `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_hourly_geo_visits_poi'] }}` t3 
    -- `storage-dev-olvin-com.visits_estimation.adjustments_hourly_geo_visits_poi`t3
    using (fk_sgplaces, hour_week)
),
missing_pois as (
  SELECT distinct fk_sgplaces, t3.tier_id_olvin from (select fk_sgplaces, hour_week from combine_inputs ) t1
  right join 
    `{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_places_dynamic_output_table'] }}` t2
  -- `storage-dev-olvin-com.visits_estimation.adjustments_places_dynamic_output` t2
  using(fk_sgplaces)
  left join 
  `{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}` t3
  -- `storage-dev-olvin-com.visits_estimation.poi_class` t3
  using(fk_sgplaces)
  where t1.hour_week is null

),
group_missing_pois as (
  select fk_sgplaces, hour_week, tier_id_olvin, hourly_visits_scaled as group_hourly_visits, 0 as poi_hourly_visits, 0 as ratio_visits_detected from missing_pois
  left join 
  (select distinct tier_id_olvin, hour_week, hourly_visits_scaled FROM 
  `{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_hourly_geo_visits_grouped'] }}`)
  -- `storage-dev-olvin-com.visits_estimation.adjustments_hourly_geo_visits_grouped` )
  using (tier_id_olvin)

),
join_missing_pois as (
select * from combine_inputs 
union all
select * from group_missing_pois
),
predicted_values as (
  SELECT
    fk_sgplaces,
    hour_week,
    CAST(group_hourly_visits as FLOAT64) group_hourly_visits,
    (1 - CAST("{{params['theta']}}" as FLOAT64)  * SQRT(ratio_visits_detected)) * group_hourly_visits +  CAST("{{params['theta']}}" as FLOAT64)   * SQRT(ratio_visits_detected) * poi_hourly_visits AS predicted_visits
  FROM
    join_missing_pois
),
fill_missing_hours as (
  select
    fk_sgplaces,
    hour_week,
    CASE
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
    END as day_of_week,
    IFNULL(predicted_visits, 0) predicted_visits
  FROM
    (
      select
        distinct fk_sgplaces
      from
        predicted_values
    ),
    (
      select
        distinct hour_week
      from
        predicted_values
    )
    left join predicted_values using (fk_sgplaces, hour_week) --select fk_sgplaces, count(*) num_rows from fill_missing_hours group by 1 having count(*)!=168
),
week_array_table as (
  select
    fk_sgplaces,
    week_array
  from
    (
      select
        *,
        count(week_array) over() as count_final
      from
        (
          select
            og.fk_sgplaces,
            og.count_og,
            case
              when adj.week_array is null then og.week_array
              else adj.week_array
            end as week_array
          from
            (
              select
                pid as fk_sgplaces,
                week_array,
                count(week_array) over () as count_og
              from
                week_array_table_temp
            ) og
            left join (
              select
                fk_sgplaces,
                week_array
              from
                `{{ var.value.prod_project }}.{{ params['places_dataset'] }}.{{ params['openings_adjustment_week_array_table'] }}`
                -- `storage-prod-olvin-com.sg_places.openings_adjustment_week_array`
            ) adj using(fk_sgplaces)
        )
    )
  WHERE
    IF(
      count_og = count_final,
      TRUE,
      ERROR(
        FORMAT(
          "count_og  %d and count_final %d are not the same.",
          count_og,
          count_final
        )
      )
    )
),
adding_week_array as (
  select
    *
  from
    fill_missing_hours
    left join week_array_table using (fk_sgplaces)
),
adding_opening_hour as (
  SELECT
    *
  EXCEPT
(week_array),
    IFNULL(week_array [
    OFFSET
      (hour_week)], 1) AS poi_opened
  FROM
    (
      SELECT
        *,
      FROM
        adding_week_array
    )
),
calculate_opening_hour as (
  SELECT
    *,
    if(
        ((predicted_visits = 0) and (poi_opened=1)),
        0.000000001,
        (predicted_visits * poi_opened) 
      )
      as predicted_visits_with_opening
  from
    adding_opening_hour
),
scale_by_day as (
  SELECT
    *,
    IFNULL(
      predicted_visits_with_opening / IF(sum_daily_visits = 0, null, sum_daily_visits),
      0
    ) scaled_visits
  FROM
    (
      select
        *,
        sum(predicted_visits_with_opening) OVER (PARTITION BY fk_sgplaces, day_of_week) sum_daily_visits
      from
        calculate_opening_hour
    )
),
get_day_of_week AS (
  SELECT
    fk_sgplaces,
    local_date,
    EXTRACT(
      dayofweek
      FROM
        local_date
    ) AS day_of_week,
    visits
  FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_places_dynamic_output_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.adjustments_places_dynamic_output` 
    t1
),
output as (
  SELECT
    fk_sgplaces,
    local_date,
    mod(hour_week, 24) as local_hour,
    CAST(scaled_visits * visits as INT64) as visits
  FROM
    get_day_of_week t1
    left join scale_by_day t2 using (fk_sgplaces, day_of_week)
),

tests as (
  select * from output,
  (
    select count(distinct fk_sgplaces) as poi_count_start from  get_day_of_week
  ),
    (
    select count(distinct fk_sgplaces) as poi_count_end from output
  ),
      (
    select count(*) count_distinct_start from (select distinct fk_sgplaces, local_date from get_day_of_week ) 
  ),
   (
    select count(*) count_distinct_end from (select distinct fk_sgplaces, local_date from output ) 
  ),
  (
    select count(*) count_start from get_day_of_week 
  ),
    (
    select count(*) count_end from output
  )

)
select * except(poi_count_start, poi_count_end, count_distinct_start, count_distinct_end, count_start, count_end)
from
  tests 

WHERE IF(
(poi_count_start = poi_count_end) and (count_distinct_start = count_distinct_end) and (count_end = 24*count_start),
TRUE,
ERROR(FORMAT("distinct_pois_input_table  %d and distinct_pois_origin %d are not the same. Or count_distinct_start  %d > count_distinct_end %d . Or count_start %d is not 24* count_end %d ", poi_count_start, poi_count_end, count_distinct_start, count_distinct_end, count_start, count_end)
)
);


CREATE
OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_output_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
AS
select fk_sgplaces, local_date, CAST(sum(visits) as INT64) as visits from `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_output_hourly_table'] }}` 
group by 1,2;