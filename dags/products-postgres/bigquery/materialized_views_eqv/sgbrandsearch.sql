CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGBrandSearch_table'] }}`
AS


SELECT
  b.name AS full_name,
  b.pid AS fk_sgbrands,
  bm.place_count,
  activity
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` b
INNER JOIN (
  -- brands nationwide
  SELECT fk_sgbrands, COUNT(*) as place_count
  FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` ON pid = fk_sgplaces
  WHERE activity IN ('active', 'limited_data')
  GROUP BY fk_sgbrands
  -- UNION ALL
  -- TO DO: brands in state
  -- TO DO: brands in city
) bm ON bm.fk_sgbrands = b.pid
ORDER BY b.name