CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['zipcoderaw_table'] }}`
AS

SELECT
  LPAD(CAST(z.pid AS STRING), 5, '0') as pid,
  -- ST_ASTEXT(ST_GEOGFROMTEXT(z.point)) AS point,
  ST_ASTEXT(ST_Simplify(ST_GEOGFROMTEXT(z.polygon), 0.0005)) AS polygon,
  ST_ASTEXT(ST_GEOGFROMTEXT(
    CONCAT(
      'POLYGON((',
      bounding_box_coords.xmin, ' ', bounding_box_coords.ymin, ', ',
      bounding_box_coords.xmin, ' ', bounding_box_coords.ymax, ', ',
      bounding_box_coords.xmax, ' ', bounding_box_coords.ymax, ', ',
      bounding_box_coords.xmax, ' ', bounding_box_coords.ymin, ', ',
      bounding_box_coords.xmin, ' ', bounding_box_coords.ymin, '))'
    )
  )) AS bounding_box,
  z.primary_city,
  z.county,
  z.state,
  z.population,
  z.active_places_count,
  z.monthly_visits_availability,
  z.patterns_availability,
  CASE
    -- treat `watch_list` as `active`
    WHEN a.activity IN ('active', 'watch_list') THEN 'active'
    ELSE a.activity
    END AS activity
FROM (
  SELECT *, ST_BOUNDINGBOX(ST_GEOGFROMTEXT(polygon)) AS bounding_box_coords
  FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['zipcoderaw_table'] }}`
) z
INNER JOIN
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['zipcodeactivity_table'] }}` a
  ON a.fk_zipcodes = z.pid