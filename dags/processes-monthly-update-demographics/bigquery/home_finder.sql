WITH
  raw_coordinates_table AS (
  SELECT
    device_id,
        TO_HEX(CAST(( -- We want the final result in hexadecimal
        SELECT
          STRING_AGG(
            CAST(S2_CELLIDFROMPOINT(nighttime_location, 7) >> bit & 0x1 AS STRING), '' ORDER BY bit DESC) -- S2_CELLIDFROMPOINT returns an integer, convert to binary. 8 is the level of the cell
            FROM UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
        ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string, BYTES format is required to use TO_HEX
      )
    ) AS home_s2_token,
        TO_HEX(CAST(( -- We want the final result in hexadecimal
        SELECT
          STRING_AGG(
            CAST(S2_CELLIDFROMPOINT(IF(day_of_week > 1 AND day_of_week < 7, daytime_location, NULL), 7) >> bit & 0x1 AS STRING), '' ORDER BY bit DESC) -- S2_CELLIDFROMPOINT returns an integer, convert to binary. 8 is the level of the cell
            FROM UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
        ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string, BYTES format is required to use TO_HEX
      )
    ) AS work_s2_token,
    ST_X(nighttime_location) AS longitude_night,
    ST_Y(nighttime_location) AS latitude_night,
    IF(day_of_week > 1 AND day_of_week < 7, ST_X(daytime_location), NULL) AS longitude_day,
    IF(day_of_week > 1 AND day_of_week < 7, ST_Y(daytime_location), NULL) AS latitude_day,
    DATE_TRUNC(local_date, MONTH) AS local_date_org,
  FROM
    (SELECT device_id, ANY_VALUE(daytime_location) AS daytime_location, ANY_VALUE(nighttime_location) AS nighttime_location, local_date, EXTRACT(DAYOFWEEK FROM local_date) AS day_of_week
     FROM 
      `{{ params['project'] }}.{{ params['raw_visits_dataset'] }}.*`
    WHERE
    local_date >= "{{ execution_date.subtract(months=2).start_of('month').strftime('%Y-%m-%d') }}" AND
    local_date < "{{ execution_date.add(months=1).start_of('month').strftime('%Y-%m-%d') }}"
    GROUP BY device_id, local_date
    )
  ),
template_table AS (
    SELECT DISTINCT device_id, local_date_org AS local_date FROM raw_coordinates_table
),

coordinates_table AS (
  SELECT
    home_s2_token,
    work_s2_token,
    local_date,
    device_id,
    longitude_night,
    latitude_night,
    longitude_day,
    latitude_day,
  FROM
    (
      SELECT
        local_date_org,
        home_s2_token,
        work_s2_token,
        device_id,
        longitude_night,
        latitude_night,
        longitude_day,
        latitude_day,
      FROM raw_coordinates_table)
    CROSS JOIN UNNEST(
      GENERATE_DATE_ARRAY(
        local_date_org,
        DATE_ADD(local_date_org, INTERVAL 2 MONTH),
        INTERVAL 1 MONTH
      )
    ) AS local_date
  WHERE
    local_date >= "{{ execution_date.start_of('month').strftime('%Y-%m-%d') }}" AND
    local_date < "{{ execution_date.add(months=1).start_of('month').strftime('%Y-%m-%d') }}"
),
device_home_count_table AS(
    SELECT
      device_id,
      home_s2_token,
      local_date,
      COUNT(*) AS count_s2,
    FROM
      coordinates_table
    WHERE
      home_s2_token IS NOT NULL
    GROUP BY
      device_id,
      home_s2_token,
      local_date),
home_area_table AS(
  SELECT
    ANY_VALUE(home_s2_token) AS home_s2_token,
    device_id,
    local_date
  FROM (
    SELECT
      device_id,
      local_date,
      MAX(count_s2) AS count_s2,
    FROM
        device_home_count_table
    GROUP BY
      device_id,
      local_date )
  JOIN (
    SELECT
      count_s2,
      home_s2_token,
      local_date,
      device_id
    FROM
      device_home_count_table )
  USING
    (device_id,
      local_date,
      count_s2)
  GROUP BY
    device_id, local_date),
device_work_count_table AS(
    SELECT
      device_id,
      work_s2_token,
      local_date,
      COUNT(*) AS count_s2,
    FROM
      coordinates_table
    WHERE
      home_s2_token IS NOT NULL
    GROUP BY
      device_id,
      work_s2_token,
      local_date),
work_area_table AS(
  SELECT
    ANY_VALUE(work_s2_token) AS work_s2_token,
    device_id,
    local_date
  FROM (
    SELECT
      device_id,
      local_date,
      MAX(count_s2) AS count_s2,
    FROM
        device_home_count_table
    GROUP BY
      device_id,
      local_date )
  JOIN (
    SELECT
      count_s2,
      work_s2_token,
      local_date,
      device_id
    FROM
      device_work_count_table )
  USING
    (device_id,
      local_date,
      count_s2)
  GROUP BY
    device_id, local_date),
home_location_table AS (
        SELECT
          home_s2_token,
          local_date,
          device_id,
      ST_GEOGPOINT(APPROX_QUANTILES(longitude_night, 2)[OFFSET (1)],
        APPROX_QUANTILES(latitude_night, 2)[OFFSET (1)]) AS home_point
        FROM
          coordinates_table
        JOIN home_area_table
          USING (home_s2_token, local_date, device_id)
        GROUP BY home_s2_token, local_date, device_id
),
work_location_table AS (
        SELECT
          work_s2_token,
          local_date,
          device_id,
      ST_GEOGPOINT(APPROX_QUANTILES(longitude_day, 2)[OFFSET (1)],
        APPROX_QUANTILES(latitude_day, 2)[OFFSET (1)]) AS work_point
        FROM
          coordinates_table
        JOIN work_area_table
          USING (work_s2_token, local_date, device_id)
        GROUP BY work_s2_token, local_date, device_id
)
SELECT
  *
FROM
  home_location_table
FULL OUTER JOIN
  work_location_table
USING (local_date, device_id)
FULL OUTER JOIN
  template_table
USING (local_date, device_id)
