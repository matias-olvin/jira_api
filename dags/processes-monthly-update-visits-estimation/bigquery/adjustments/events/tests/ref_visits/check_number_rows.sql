
WITH

size_ref_visits AS(
SELECT COUNT(*) as num_rows_ref_visits
FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_ref_visits_places_table'] }}`
),

size_original_visits AS(
  SELECT COUNT(*) num_rows_original
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}`
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_poi_table'] }}`
    USING(fk_sgplaces, local_date)
)

SELECT num_rows_original - num_rows_ref_visits
FROM size_original_visits
JOIN size_ref_visits
ON True