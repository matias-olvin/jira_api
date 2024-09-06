CALL `{{ var.value.env_project }}.{{ params['quality_ml_input_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['smc_quality_dataset'] }}.{{ params['model_input_complete_table'] }}",
    "{{ params['geoscaling_input_start_date'] }}",
    "{{ params['geoscaling_input_end_date'] }}",
    "{{ var.value.env_project }}.{{ params['smc_poi_visits_dataset'] }}"
)