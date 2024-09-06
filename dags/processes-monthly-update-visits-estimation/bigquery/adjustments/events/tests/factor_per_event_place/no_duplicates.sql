
SELECT COUNTIF(duplicates > 1)
FROM (
    SELECT fk_sgplaces, local_date, COUNT(*) AS duplicates
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_places_table'] }}`
    GROUP BY fk_sgplaces, local_date
)