CREATE OR REPLACE TABLE
    `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}`
COPY
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_estimation_dataset']}}-{{ params['tier_events_table'] }}`
