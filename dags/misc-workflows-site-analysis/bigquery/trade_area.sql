CREATE OR REPLACE TABLE 
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['trade_area_table'] }}_{{ source }}` AS
WITH places_table AS (
  SELECT
    ST_CENTROID_AGG(lat_long_visit_point) AS ref_point,
  FROM
    `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_table'] }}`
),
device_locations_table AS (
  SELECT
    *
  EXCEPT(visit_score),
    visit_score / SUM(visit_score) OVER () AS visit_score
  FROM
    (
      SELECT
        device_id,
        local_date,
        TO_HEX(
          CAST(
            (
              -- We want teh final result in hexadecimal
              SELECT
                STRING_AGG(
                  CAST(
                    S2_CELLIDFROMPOINT({{ source }}_point, 14) >> bit & 0x1 AS STRING
                  ),
                  ''
                  ORDER BY
                    bit DESC
                ) -- S2_CELLIDFROMPOINT(lat_lon_point, 14) gices an integer, convert to binary
              FROM
                UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
            ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string
          )
        ) AS s2_token,
        {{ source }}_point,
        ST_DISTANCE(
          {{ source }}_point,
          ref_point
        ) AS distance,
        visit_score
      FROM
        (SELECT {{ source }}_point, device_id, local_date,
        visit_score FROM
            `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`)
        CROSS JOIN places_table
      WHERE
        {{ source }}_point IS NOT NULL
    )
),
cell_values AS (
  SELECT
    total_s2_cell / SUM(total_s2_cell) OVER () * 100 AS cell_percentage,
    total_s2_cell AS total_visits,
    s2_token,
    LOG(total_s2_cell) AS view_number,
    --distance,
  FROM(
      SELECT
        s2_token,
        SUM(visit_score) AS total_s2_cell,
        AVG(distance) AS distance,
      FROM
        device_locations_table
      GROUP BY
        s2_token
    )
),
-- SELECT
-- cell_percentage,
-- cum_percentage,
-- total_visits,
-- CASE
--     WHEN cum_percentage < 20 THEN "20"
--     WHEN cum_percentage < 40 THEN "40"
--     WHEN cum_percentage < 60 THEN "60"
--     ELSE "100"
-- END AS region,
-- distance,
-- s2_token,
-- FROM
-- (SELECT
-- s2_token,
-- cell_percentage,
-- total_visits,
-- SUM(cell_percentage) OVER (ORDER BY 1/distance DESC, cell_percentage DESC) AS cum_percentage,
-- distance,
-- FROM cell_values)
-- UNION ALL
-- SELECT
-- 0, 0, 0, "0", 0, "8888888888888800"
point_hulls AS (
  SELECT
    device_id,
    ST_BUFFERWITHTOLERANCE(
      ST_CONVEXHULL(
        (
          ST_UNION(
            ARRAY_AGG({{ source }}_point) OVER (
              ORDER BY
                cell_percentage / GREATEST(1, distance - 10000) DESC,
                cell_percentage DESC,
                distance
            )
          )
        )
      ),
      100,
      20
    ) AS point_hull,
    distance
  FROM
    device_locations_table
    JOIN cell_values USING(s2_token)
),
final_polygons AS (
  SELECT
    region,
    cum_percentage,
    point_hull
  FROM
    (
      SELECT
        point_hull,
        region,
        cum_percentage,
        ROW_NUMBER() OVER (
          PARTITION BY region
          ORDER BY
            cum_percentage
        ) AS region_rank
      FROM
        (
          SELECT
            point_hull,
            cum_percentage,
            CASE
              WHEN cum_percentage < 0.1 THEN 0
              WHEN cum_percentage < 0.2 THEN 10
              WHEN cum_percentage < 0.3 THEN 20
              WHEN cum_percentage < 0.4 THEN 30
              WHEN cum_percentage < 0.5 THEN 40
              WHEN cum_percentage < 0.6 THEN 50
              WHEN cum_percentage < 0.7 THEN 60
              WHEN cum_percentage < 0.8 THEN 70
              WHEN cum_percentage < 0.9 THEN 80
              ELSE 90
            END AS region,
          FROM
            (
              SELECT
                point_hull,
                hull_count AS cum_percentage,
                distance,
              FROM
                (
                  SELECT
                    device_id,
                    ANY_VALUE(point_hull) AS point_hull,
                    SUM(visit_score) AS hull_count,
                    ANY_VALUE(distance) AS distance
                  FROM
                    point_hulls
                    JOIN (
                      SELECT
                        {{ source }}_point,
                        visit_score
                      FROM
                        device_locations_table
                    ) ON ST_COVERS(point_hull, {{ source }}_point)
                  GROUP BY
                    device_id
                )
            )
        )
    )
  WHERE
    region_rank = 1
)
-- SELECT
--   region,
--   cum_percentage,
--   ST_DIFFERENCE(
--     point_hull,
--     IFNULL(remove_polygon, ST_GEOGPOINT(0, 0))
--   ) AS point_hull,
--   --point_hull
-- FROM
--   (
--     SELECT
--       table_1.region,
--       AVG(table_1.cum_percentage) AS cum_percentage,
--       ANY_VALUE(table_1.point_hull) AS point_hull,
--       ST_UNION_AGG(table_2.point_hull) AS remove_polygon
--     FROM
--       final_polygons AS table_1
--       LEFT JOIN final_polygons AS table_2 ON table_2.region < table_1.region
--       AND table_1.fk_met_areas = table_2.fk_met_areas
--       AND table_2.region <> 0
--     WHERE
--       table_1.region <> 0
--     GROUP BY
--       region
--   )
SELECT
  region,
  cum_percentage,
  point_hull
    FROM
      final_polygons
ORDER BY
  region DESC
