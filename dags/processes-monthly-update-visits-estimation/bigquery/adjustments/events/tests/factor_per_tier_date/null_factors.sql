
SELECT COUNTIF(factor IS NULL)
FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_tier_table'] }}`