CREATE OR REPLACE TABLE `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['model_evaluation_visits_to_malls_table'] }}` AS
SELECT
  *
FROM
  ML.EVALUATE(MODEL `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['visits_to_malls_model'] }}`, (
  SELECT
    GLA,
    TOTSTORES,
    COALESCE(LEVELS,1) AS LEVELS,
    adjusted_visitors_annually_M AS label
  FROM
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['visits_to_malls_dataset'] }}-{{ params['visits_to_malls_training_data_table'] }}`
  WHERE
    adjusted_visitors_annually_M IS NOT NULL));