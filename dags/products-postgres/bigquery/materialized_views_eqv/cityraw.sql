CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['cityraw_table'] }}`
AS

SELECT
    c.city_id,
    c.city as name,
    c.state,
    c.state_abbreviation,
    ST_ASTEXT(ST_Simplify(c.polygon, 0.0005)) AS polygon,
    ST_ASTEXT(bounding_box) AS bounding_box,
    -- c.centrepoint AS centre_point,
    c.active_places_count,
    c.zipcodes,
    c.monthly_visits_availability,
    c.patterns_availability
FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['cityraw_table'] }}` c
ORDER BY name