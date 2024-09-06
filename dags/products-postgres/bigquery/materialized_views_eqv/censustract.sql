CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['censustract_table'] }}`
AS

SELECT
    c.pid,
    c.county,
    c.state,
    c.ruca,
    c.population_density,
    ST_ASTEXT(ST_Simplify(ST_GEOGFROMTEXT(c.polygon), 0.0005)) AS polygon
FROM
      `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['censustract_table'] }}`  c
ORDER BY c.state