DELETE FROM
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_decrease_mom_table'] }}`
WHERE run_date=DATE('{{ds}}')
;

INSERT INTO
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['brand_decrease_mom_table'] }}`


WITH

visits_brand_month AS(
SELECT fk_sgbrands, DATE_TRUNC(local_date, MONTH) AS local_month, visits
FROM
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_places_dynamic_output_table'] }}`
INNER JOIN(
  SELECT pid AS fk_sgplaces, fk_sgbrands
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
)
USING(fk_sgplaces)
),

agg_visits_brand_month AS(
  SELECT fk_sgbrands, local_month, SUM(visits) AS visits
  FROM visits_brand_month
  GROUP BY fk_sgbrands, local_month
),

monthly_visits_increase AS (
  SELECT
    visits / NULLIF((LAG(visits, 1) OVER (PARTITION BY fk_sgbrands ORDER BY fk_sgbrands, local_month)),0) AS increase,
    fk_sgbrands,
    -- this is because it will jump from poi to poi
      LAG(fk_sgbrands, 1) OVER (PARTITION BY fk_sgbrands ORDER BY fk_sgbrands, local_month) AS fk_sgbrands_next,
      local_month

  FROM (
    SELECT
      *
    FROM
      agg_visits_brand_month
    WHERE
      local_month > '2021-09-01'
      AND local_month < DATE_ADD(DATE('{{ds}}') , INTERVAL 5 MONTH)
    )
),

decrease_month_to_month AS (
  SELECT
    fk_sgbrands,
    local_month,
    increase,
    FROM
      monthly_visits_increase
    WHERE
      increase < 0.7
      and EXTRACT(MONTH FROM local_month) != 1 -- overlooks the decrease after Christmas season
      AND NOT (

        EXTRACT(MONTH FROM local_month) = 11 -- overlooks the decrease from October to November in specific brand
        AND fk_sgbrands <>'SG_BRAND_f0522c7c6ecb0594fd2a2e2f693eb3d9' -- Party City

      )
      and fk_sgbrands = fk_sgbrands_next
      )


SELECT
  DATE('{{ds}}') AS run_date,
  fk_sgbrands, brand, local_month,
  100*(increase - 1) AS perc_change
FROM
  decrease_month_to_month
LEFT JOIN(
  SELECT pid AS fk_sgbrands, name AS brand
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
)
USING(fk_sgbrands)
