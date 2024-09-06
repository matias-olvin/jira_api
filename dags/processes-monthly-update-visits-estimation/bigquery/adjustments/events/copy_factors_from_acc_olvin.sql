CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_tier_table'] }}`
COPY
  `{{ var.value.sns_project }}.{{ var.value.accessible_by_olvin }}.{{ params['visits_estimation_dataset']}}-{{ params['factor_tier_table']}}`
