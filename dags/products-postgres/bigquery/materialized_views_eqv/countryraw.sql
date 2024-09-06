CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['countryraw_table'] }}`
AS

SELECT
    c.country,
    c.country_abbreviation,
    ST_ASTEXT(ST_Simplify(c.polygon, 0.0005)) AS polygon,
    ST_ASTEXT(bounding_box) AS bounding_box,
    -- c.centrepoint AS centre_point
FROM
      `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['countryraw_table'] }}` c