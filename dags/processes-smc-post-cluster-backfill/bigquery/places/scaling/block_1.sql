CALL `{{ var.value.env_project }}.{{ params['block_1_scaling_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['smc_poi_visits_staging_dataset'] }}.{{ params['block_1_output_table'] }}_{{ ds }}",
    "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}",
    "{{ var.value.env_project }}.{{ params['smc_places_dataset'] }}.{{ params['places_filter_table'] }}",
    "{{ var.value.env_project }}.{{ params['demographics_dataset'] }}.{{ params['zipcode_demographics_table'] }}",
    "{{ var.value.env_project }}.{{ params['device_zipcodes_dataset'] }}",
    "{{ var.value.env_project }}.{{ params['poi_visits_dataset'] }}",
    "{{ var.value.env_project }}.{{ params['base_table_dataset'] }}.{{ params['sg_categories_match_old_table'] }}",
    "{{ var.value.env_project }}.{{ params['base_table_dataset'] }}.{{ params['naics_code_subcategories'] }}",
    "{{ var.value.env_project }}.{{ params['smc_places_dataset'] }}.{{ params['places_data_table'] }}",
    "{{ var.value.env_project }}.{{ params['original_regressors_dataset'] }}.{{ params['weather_collection_table'] }}",
    "{{ var.value.env_project }}.{{ params['new_regressors_dataset'] }}.{{ params['weather_dictionary'] }}",
    "{{ var.value.env_project }}.{{ params['original_regressors_dataset'] }}.{{ params['holidays_collection_table'] }}",
    "{{ var.value.env_project }}.{{ params['new_regressors_dataset'] }}.{{ params['holidays_dictionary'] }}",
    "{{ var.value.env_project }}.{{ params['visit_share_dataset'] }}.{{ params['visits_share_model'] }}",
    "{{ var.value.env_project }}.{{ params['visit_share_dataset'] }}.{{ params['prior_distance_function'] }}"
);