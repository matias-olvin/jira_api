-- CREATION OF MISSING BRANDS (NEW SMC BRANDS THAT DO NOT SHOW IN PRIOR BRAND VISITS TABLE)
CREATE
OR REPLACE TABLE `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['missing_brands_table'] }}` AS
SELECT
  name,
  pid,
  sub_category
FROM
  `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
WHERE
  pid NOT IN (
    SELECT
      fk_sgbrands
    FROM
      `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['prior_brand_visits_table'] }}`
  );