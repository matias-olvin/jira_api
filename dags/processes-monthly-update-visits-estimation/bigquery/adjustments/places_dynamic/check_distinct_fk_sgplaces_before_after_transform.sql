ASSERT (
    SELECT COUNT(DISTINCT fk_sgplaces)
    FROM `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_volume_output_table'] }}`
) = (
    SELECT COUNT(DISTINCT fk_sgplaces)
    FROM `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_places_dynamic_output'] }}`
) AS "Number of distinct fk_sgplaces values is not the same after places_dynamic adjustment. Table name: {{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_places_dynamic_output'] }}";