CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlacePatternVisitsRaw_table'] }}`
AS

SELECT p.fk_sgplaces, p.local_date, p.time_of_day,
  p.day_of_week, sb.tenant_type, sb.name as brand_name, sb.luxury,
  sg.region, sg.city, sg.postal_code, sg.fk_sgbrands
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlacePatternVisitsRaw_table'] }}` p
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` sg
  ON sg.pid = p.fk_sgplaces
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` sb
  ON sb.pid = sg.fk_sgbrands
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` a
  ON a.fk_sgplaces = p.fk_sgplaces
WHERE a.activity IN('active', 'limited_data')
ORDER BY p.fk_sgplaces, sg.fk_sgbrands, p.local_date
