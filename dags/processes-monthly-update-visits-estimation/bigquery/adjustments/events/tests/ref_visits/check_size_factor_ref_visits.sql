
WITH

size_ref_visits AS(
    SELECT COUNT(*) as num_rows_ref_visits
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_ref_visits_places_table'] }}`
),

size_factor_per_poi AS(
    SELECT COUNT(*) num_rows_factor
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_places_table'] }}`
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_ref_visits_places_table'] }}`
    USING(fk_sgplaces, local_date)
)

SELECT num_rows_factor - num_rows_ref_visits
FROM size_factor_per_poi
JOIN size_ref_visits
ON True