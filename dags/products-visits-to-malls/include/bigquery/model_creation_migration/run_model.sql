CREATE OR REPLACE TABLE `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['visits_to_malls_model_output'] }}` AS
SELECT
  pid,
  name,
  GLA,
  TOTSTORES,
  LEVELS,
  adjusted_visitors_annually_M,
  CASE
    WHEN adjusted_visitors_annually_M IS NULL THEN ROUND(predicted.predicted_label,2)
    ELSE adjusted_visitors_annually_M
  END AS predicted_visitors_annually_M
FROM
  ML.PREDICT(
      MODEL `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['visits_to_malls_model'] }}`,
      (SELECT 
          pid, 
          name,
          GLA, 
          TOTSTORES, 
          LEVELS, 
          adjusted_visitors_annually_M 
       FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['visits_to_malls_dataset'] }}-{{ params['visits_to_malls_training_data_table'] }}`)
    ) AS predicted;