-- CREATION OF MISSING SUBCATEGORIES (NEW SMC SUBCATEGORIES THAT DO NOT SHOW IN PRIOR BRAND VISITS TABLE)
CREATE
OR REPLACE TABLE `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['missing_sub_categories_table'] }}` AS
SELECT DISTINCT
  (sub_category)
FROM
  `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['missing_brands_table'] }}`
WHERE
  sub_category NOT IN (
    SELECT DISTINCT
      (sub_category)
    FROM
      `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['prior_brand_visits_table'] }}`
    WHERE
      sub_category IS NOT null
  );