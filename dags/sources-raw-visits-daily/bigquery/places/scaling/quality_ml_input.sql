CALL `{{ params['project'] }}.{{ params['quality_ml_input_procedure'] }}`(
    "{{ params['project'] }}.{{ params['quality_dataset'] }}.{{ params['model_input_complete_table'] }}",
    "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}",
    "{{ execution_date.subtract(days=(var.value.latency_days_visits|int-1)).strftime('%Y-%m-%d') }}",
    "{{ params['project'] }}.{{ params['poi_visits_dataset'] }}"
)