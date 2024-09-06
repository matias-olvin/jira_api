CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['brandstatemonthlyvisits_table'] }}`
AS


SELECT fk_sgbrands, state, local_date, SUM(visits) AS visits
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
INNER JOIN(
SELECT pid AS fk_sgplaces, fk_sgbrands, region AS state
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
WHERE fk_sgbrands IN(
  SELECT pid
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}`
)
)
USING(fk_sgplaces)
WHERE fk_sgplaces IN(
SELECT fk_sgplaces
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}`
WHERE activity IN('active', 'limited_data')
)
GROUP BY fk_sgbrands, local_date, state