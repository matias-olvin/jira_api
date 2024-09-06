SELECT COUNTIF(visits IS NULL)
FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output_table'] }}`