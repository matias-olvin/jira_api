CREATE OR REPLACE TABLE
  `{{ params['bigquery-project'] }}.{{ params['postgres-mv-rt-dataset'] }}.{{ params['sgplacemonthlyvisitsraw-table'] }}`
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
  `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['sgplacemonthlyvisitsraw-table'] }}` v
INNER JOIN
  `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceraw-table'] }}` p
  ON p.pid = v.fk_sgplaces
INNER JOIN
  `{{ params['bigquery-project'] }}.{{ params['postgres-batch-dataset'] }}.{{ params['sgplaceactivity-table'] }}` a
  ON a.fk_sgplaces = v.fk_sgplaces
WHERE a.activity IN ('active', 'limited_data')
ORDER BY p.fk_sgbrands, v.fk_sgplaces, v.local_date