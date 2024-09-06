
WITH

size_adjustments_events_visits AS(
    SELECT COUNT(*) as num_rows_events
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output_table'] }}`
),

size_quality_output_visits AS(
    SELECT COUNT(*) num_rows_quality
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}`
)

SELECT num_rows_events - num_rows_quality
FROM size_adjustments_events_visits
JOIN size_quality_output_visits
ON True