CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
  -- `storage-prod-olvin-com.postgres_materialize_views.SGPlacMonthlyVisitsRaw`
AS

SELECT
    p.fk_sgbrands,
    p.region,
    p.city,
    p.name,
    p.street_address,
    p.postal_code AS fk_zipcodes,
    p.latitude,
    p.longitude,
    p.opening_date,
    p.closing_date,
    p.opening_status,
    v.fk_sgplaces,
    v.local_date,
    v.visits,
    CASE
      -- treat `watch_list` as `active`
      WHEN a.activity IN ('active', 'watch_list') THEN 'active'
      ELSE a.activity
    END AS activity
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}` v
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` p
  ON p.pid = v.fk_sgplaces
INNER JOIN
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` a
  ON a.fk_sgplaces = v.fk_sgplaces
WHERE a.activity IN ('active', 'limited_data')
ORDER BY p.fk_sgbrands, v.fk_sgplaces, v.local_date