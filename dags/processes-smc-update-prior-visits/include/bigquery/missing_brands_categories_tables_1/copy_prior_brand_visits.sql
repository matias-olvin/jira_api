-- Creates a copy of prior_brand_visits in smc dataset
CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}` AS
SELECT
    *
FROM
    `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['prior_brand_visits_table'] }}`;