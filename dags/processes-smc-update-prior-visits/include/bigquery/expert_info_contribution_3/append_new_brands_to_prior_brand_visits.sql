MERGE INTO
  `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}` AS target
USING `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}` AS source ON target.fk_sgbrands = source.pid
WHEN MATCHED THEN
UPDATE SET
  fk_sgbrands = source.pid,
  sub_category = source.sub_category,
  median_category_visits = source.median_category_visits,
  category_median_visits_range = source.category_median_visits_range,
  median_brand_visits = source.median_brand_visits
WHEN NOT MATCHED THEN
INSERT
  (
  fk_sgbrands,
  sub_category,
  median_category_visits,
  category_median_visits_range,
  median_brand_visits
  )
VALUES
  (
  source.pid,
  source.sub_category,
  source.median_category_visits,
  source.category_median_visits_range,
  source.median_brand_visits
  );