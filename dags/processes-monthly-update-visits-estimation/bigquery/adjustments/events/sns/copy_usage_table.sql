CREATE OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['visits_estimation_ground_truth_supervised_dataset'] }}_staging.{{ params['adjustments_events_usage_table'] }}` 
COPY `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_estimation_ground_truth_supervised_dataset'] }}-{{ params['adjustments_events_usage_table'] }}`;