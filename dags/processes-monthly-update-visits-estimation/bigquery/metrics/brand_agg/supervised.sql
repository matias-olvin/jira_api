DELETE FROM
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brands_visits_table'] }}`
WHERE run_date=DATE('{{ds}}')
  AND step='1-supervised_core'
;

INSERT INTO
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brands_visits_table'] }}`

WITH

poi_brand_visits AS(
  SELECT fk_sgplaces, fk_sgbrands, local_date, visits
  FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_core_table'] }}`
  INNER JOIN(
      SELECT pid AS fk_sgplaces, fk_sgbrands
      FROM
        `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  )
  USING(fk_sgplaces)
),

total_brand_visits AS(
  SELECT fk_sgbrands, local_date, SUM(visits) total_visits
  FROM poi_brand_visits
  GROUP BY fk_sgbrands, local_date
)

SELECT DATE('{{ds}}') AS run_date,
       local_date, fk_sgbrands, brand, total_visits,
       '1-supervised_core' AS step
FROM total_brand_visits
LEFT JOIN(
  SELECT pid AS fk_sgbrands, name AS brand
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
)
USING(fk_sgbrands)