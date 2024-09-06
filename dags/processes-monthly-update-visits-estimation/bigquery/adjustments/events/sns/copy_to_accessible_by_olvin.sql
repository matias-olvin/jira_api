CREATE OR REPLACE TABLE
  `{{ var.value.sns_project }}.{{ var.value.accessible_by_olvin }}.{{ params['visits_estimation_dataset']}}-{{ params['factor_tier_table']}}`
COPY 
  `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['factor_tier_table'] }}`;
