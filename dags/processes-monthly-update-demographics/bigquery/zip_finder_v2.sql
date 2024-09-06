WITH device_homes AS (
  SELECT
    local_date,
    device_id,
    home_point,
    fk_zipcodes
FROM

    (
      SELECT
        local_date,
        device_id,
        ANY_VALUE(home_point) AS home_point
      FROM
        `{{ params['project'] }}.{{ params['device_homes_dataset'] }}.*`
      WHERE
      local_date >= "{{ execution_date.start_of('month').strftime('%Y-%m-%d') }}" AND
      local_date < "{{ execution_date.add(months=1).start_of('month').strftime('%Y-%m-%d') }}"
    AND home_point IS NOT NULL
      GROUP BY
        local_date,
        device_id
    )
    JOIN (
      SELECT
        pid AS fk_zipcodes,
        ST_GEOGFROMTEXT(polygon) AS polygon
      FROM
        `{{ params['project'] }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
    )
    ON ST_CONTAINS(polygon, home_point)
),
zip_code_info AS (
  SELECT
    *
  EXCEPT(point),
  ST_GEOGFROMTEXT(point) AS zip_point
  FROM
    (
      SELECT
        zip_id,
        ANY_VALUE(point) AS point,
        ANY_VALUE(fk_zipcodes) AS fk_zipcodes
      FROM
        `{{ params['project'] }}.{{ params['static_demographics_data_v2_dataset'] }}.{{ params['zipcode_demographics_table'] }}`
      GROUP BY
        zip_id
    )
),
grouped_met_areas AS (
  SELECT
    fk_zipcodes,
    ST_UNION_AGG(zip_point) AS zip_arr
  FROM
    zip_code_info
  GROUP BY
    fk_zipcodes
),
join_zipcodes AS (
  SELECT
    device_homes.*
  EXCEPT(home_point),
    closest_zip
  FROM
    device_homes
    LEFT JOIN grouped_met_areas ON grouped_met_areas.fk_zipcodes = device_homes.fk_zipcodes,
    UNNEST([ST_CLOSESTPOINT(zip_arr,
        home_point)]) AS closest_zip
)
SELECT
  join_zipcodes.*
EXCEPT(closest_zip),
  zip_code_info.*
EXCEPT(zip_point, fk_zipcodes)
FROM
  join_zipcodes
  JOIN zip_code_info ON ST_EQUALS(
    join_zipcodes.closest_zip,
    zip_code_info.zip_point
  )