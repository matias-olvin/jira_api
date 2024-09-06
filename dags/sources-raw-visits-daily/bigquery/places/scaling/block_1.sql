CALL `{{ params['project'] }}.{{ params['block_1_scaling_procedure'] }}`(
    "{{ params['project'] }}.{{ params['poi_visits_staging_dataset'] }}.{{ params['block_1_output_table'] }}_{{ ds }}",
    "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}",
    "{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['sg_places_filter_table'] }}",
    "{{ params['project'] }}.{{ params['demographics_dataset'] }}.{{ params['zipcode_demographics_table'] }}",
    "{{ params['project'] }}.{{ params['device_zipcodes_dataset'] }}",
    "{{ params['project'] }}.{{ params['poi_visits_dataset'] }}",
    "{{ params['project'] }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sg_categories_match_old_table'] }}",
    "{{ params['project'] }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories'] }}",
    "{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['places_data_table'] }}",
    "{{ params['project'] }}.{{ params['new_regressors_dataset'] }}.{{ params['weather_collection_table'] }}",
    "{{ params['project'] }}.{{ params['new_regressors_dataset'] }}.{{ params['weather_dictionary'] }}",
    "{{ params['project'] }}.{{ params['new_regressors_dataset'] }}.{{ params['holidays_collection_table'] }}",
    "{{ params['project'] }}.{{ params['new_regressors_dataset'] }}.{{ params['holidays_dictionary'] }}",
    "{{ params['project'] }}.{{ params['visit_share_dataset'] }}.{{ params['visits_share_model'] }}",
    "{{ params['project'] }}.{{ params['visit_share_dataset'] }}.{{ params['prior_distance_function'] }}"
);