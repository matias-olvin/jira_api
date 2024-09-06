CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['stateraw_table'] }}`
AS

SELECT
  CAST(s.pid AS STRING) AS pid,
  s.name,
  s.abbreviation,
  s.latitude,
  s.longitude,
  ST_ASTEXT(ST_Simplify(ST_GEOGFROMTEXT(s.polygon), 0.0005)) AS polygon,
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
FROM (
  SELECT *, ST_BOUNDINGBOX(ST_GEOGFROMTEXT(polygon)) AS bounding_box_coords
  FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['stateraw_table'] }}`
 ) s
ORDER BY s.abbreviation