
SELECT COUNTIF(events_output IS NULL OR quality_output IS NULL)
FROM (
    SELECT fk_sgplaces, TRUE AS events_output
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output_table'] }}`
    GROUP BY fk_sgplaces
)
FULL OUTER JOIN (
    SELECT fk_sgplaces, TRUE AS quality_output
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}`
    GROUP BY fk_sgplaces
)
USING(fk_sgplaces)