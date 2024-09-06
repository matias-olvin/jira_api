CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceVisitorBrandDestinations_table'] }}`
AS

SELECT
    pid,
    brands,
    total_devices
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceVisitorBrandDestinations_table'] }}` p
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` sg
  USING(pid)
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` a
  ON a.fk_sgplaces = p.pid
WHERE a.activity IN('active', 'limited_data')
ORDER BY pid
