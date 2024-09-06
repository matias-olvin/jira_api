CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceSearch_table'] }}`
AS


SELECT
  concat(b.name, ' ', replace(p.name, b.name, '')) as name,
  concat(b.name, ' ', replace(p.name, b.name, ''), ' ', (COALESCE(p.street_address, '')), ' ', p.city, ' ', p.postal_code, ' ', (p.region)) as full_name,
  p.pid as fk_places,
  p.fk_sgbrands as fk_chains,
--  ST_ASTEXT(ST_GEOGPoint(p.longitude, p.latitude)) as point
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` p
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` b ON b.pid = p.fk_sgbrands
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` a ON a.fk_sgplaces = p.pid
WHERE a.activity IN ('active', 'limited_data')
AND p.opening_status <> 0
ORDER BY p.fk_sgbrands
