
CREATE TEMP FUNCTION MAP(expr ANY TYPE, map ANY TYPE, `default` ANY TYPE ) AS (
  IFNULL((
    SELECT result
    FROM (SELECT NULL AS search, NULL AS result UNION ALL SELECT * FROM UNNEST(map))
    WHERE search = expr), `default`)
);
CREATE TEMP FUNCTION ExpandList(input STRING) AS (
  ARRAY(
    -- Find the value before the *
    SELECT SPLIT(elem, '*')[OFFSET(0)]
    -- For each comma-separated element inside the braces
    FROM UNNEST(REGEXP_EXTRACT_ALL(input, r'[^\[\],]+')) AS elem,
    -- Repeated by the value after the *, or once if there is no *
    UNNEST(GENERATE_ARRAY(1, IFNULL(CAST(SPLIT(elem, '*')[SAFE_OFFSET(1)] AS INT64), 1))))
);


CREATE or REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_week_array_table'] }}` AS

WITH
fill_empty_open_hours as (
  SELECT
    * EXCEPT(open_hours),
    CASE
      WHEN open_hours is null THEN '{ "Mon": [["9:00", "21:00"]], "Tue": [["9:00", "21:00"]], "Wed": [["9:00", "21:00"]], "Thu": [["9:00", "21:00"]], "Fri": [["9:00", "21:00"]], "Sat": [["10:00", "21:00"]], "Sun": [["11:00", "19:00"]] }'
      ELSE replace(open_hours, r'[]', '["0:00","0:00"]')
    END AS open_hours
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
),


split_open_hours_by_day as (
  SELECT
    open_hours,
    pid,
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
    pid,
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
    pid ,
    opening_hour,
    closing_hour,
    MAP (
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
      pid,
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
    pid,
    day_of_week_int * 24 + opening_hour as opening_week_hour,
    day_of_week_int * 24 + closing_hour as closing_week_hour,
    closing_hour - opening_hour as open_length
    FROM
      unnest_array
),
transform_hours AS (
  SELECT
    pid,
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
    ExpandList(concat_opening) AS week_array
  FROM
    concat_hours
),
places_opening_hours_week_array AS (
  SELECT
    pid,
    ARRAY_AGG(x ORDER BY day_of_week_int) AS week_array
  FROM (
    SELECT
      pid,
      day_of_week_int,
      LEAST(SUM(CAST(x as INT64)),1) as x
    FROM
      create_week_array,
      UNNEST(week_array) as x with OFFSET day_of_week_int
    GROUP BY
      pid,
      day_of_week_int
  )
  group by pid
)
SELECT *
FROM
places_opening_hours_week_array
