
SELECT COUNTIF(inner_join IS NULL)
FROM (
    SELECT fk_sgplaces, TRUE AS left_join
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['tier_events_table'] }}`
    LEFT JOIN
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}`
    USING(fk_sgplaces)
)
LEFT JOIN (
    SELECT fk_sgplaces, TRUE AS inner_join
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['tier_events_table'] }}`
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}`
    USING(fk_sgplaces)
)
USING(fk_sgplaces)