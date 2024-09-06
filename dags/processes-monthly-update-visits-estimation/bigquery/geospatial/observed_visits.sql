CREATE OR REPLACE TABLE
 `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['visits_aggregated_table'] }}`
 partition by local_date
 cluster by fk_sgplaces
AS

SELECT
  fk_sgplaces,
  local_date,
  hour_ts,
  SUM(visit_score) AS visits_observed
FROM
  `{{ var.value.prod_project }}.{{ params['poi_visits_scaled_dataset'] }}.*`
  --`olvin-sandbox-20210203-0azu4n.alfonso_test.poi_visits_sample`
INNER JOIN
    (
        select
            pid as fk_sgplaces
        from
            --`storage-prod-olvin-com.sg_places.20211101`
            `{{ var.value.prod_project }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
        WHERE
        fk_sgbrands
        IS NOT NULL
    )
using (
    fk_sgplaces
    )
  WHERE local_date>="2018-01-01"
GROUP BY
  fk_sgplaces,
  local_date,
  hour_ts