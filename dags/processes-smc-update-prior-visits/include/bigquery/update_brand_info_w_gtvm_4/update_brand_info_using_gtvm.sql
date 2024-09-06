CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}` AS
WITH
  brands_ground_truth AS (
    SELECT
      brand.pid,
      ROUND(
        (ARRAY_AGG(visits_per_day ORDER BY visits_per_day))[OFFSET(DIV(ARRAY_LENGTH(ARRAY_AGG(visits_per_day)), 2))]
      ) AS median_brand_visits
    FROM
      `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.v-{{ params['smc_gtvm_dataset'] }}-{{ params['gtvm_target_agg_sns_table'] }}`
      LEFT JOIN `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` place ON place.pid = fk_sgplaces
      LEFT JOIN `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}` brand ON place.fk_sgbrands = brand.pid
    GROUP BY
      brand.pid
  )
SELECT
  fk_sgbrands,
  sub_category,
  COALESCE(median_category_visits, 10000) AS median_category_visits,
  COALESCE(category_median_visits_range, 10) AS category_median_visits_range,
  COALESCE(
    30 * gt.median_brand_visits,
    pb.median_brand_visits
  ) AS median_brand_visits
FROM
  `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}` pb
  LEFT JOIN brands_ground_truth gt ON pid = fk_sgbrands;