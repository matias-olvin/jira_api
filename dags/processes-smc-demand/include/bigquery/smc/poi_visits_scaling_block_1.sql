CALL `{{ params['block_1_scaling_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_block_1_dataset'] }}",
    "{{ params['date_start'] }}",
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['places_demand_table'] }}",
    "{{ var.value.env_project }}.{{ params['demographics_dataset'] }}.{{ params['zipcode_demographics_table'] }}",
    "{{ var.value.env_project }}.{{ params['device_zipcodes_dataset'] }}",
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_dataset'] }}",
    "{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['categories_match_table'] }}",
    "{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}",
    "{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}",
    "{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}",
    "{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_dictionary_table'] }}",
    "{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['holidays_collection_table'] }}",
    "{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['holidays_dictionary_table'] }}",
    "{{ var.value.env_project }}.{{ params['visits_share_dataset'] }}.{{ params['visits_share_model'] }}",
    "{{ var.value.env_project }}.{{ params['visits_share_dataset'] }}.{{ params['prior_distance_parameters_table'] }}"
);
