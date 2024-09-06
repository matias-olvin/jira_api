CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_estimation_dataset']}}-{{ params['tier_events_table'] }}`
COPY
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['tier_events_table'] }}`