CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['sgbrandraw_table'] }}`
AS

SELECT *
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` b
LEFT JOIN(
  SELECT
    fk_sgbrands AS pid,
    logo
  FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGBrandLogo_table'] }}`
)
USING(pid)
INNER JOIN(
  SELECT
    b.pid,
    COUNTIF(a.activity IN ('active', 'limited_data') AND p.opening_status = 1) AS place_count,
    COUNTIF(a.activity IN ('active', 'limited_data', 'no_data') AND p.opening_status = 1) AS all_places_count
  FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` b
  INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` p
    ON p.fk_sgbrands = b.pid
  INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` a
    ON a.fk_sgplaces = p.pid
  GROUP BY b.pid
)
USING(pid)
ORDER BY b.pid