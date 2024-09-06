CALL `{{ var.value.env_project }}.{{ params['quality_ml_input_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['quality_dataset'] }}.{{ params['model_input_complete_table'] }}",
    "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}",
    "{{ execution_date.subtract(days=(var.value.latency_days_visits|int-1)).strftime('%Y-%m-%d') }}",
    "{{ var.value.env_project }}.{{ params['poi_visits_dataset'] }}"
)