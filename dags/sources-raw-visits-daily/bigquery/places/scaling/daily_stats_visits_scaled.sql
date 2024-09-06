CALL `{{ params['project'] }}.{{ params['day_stats_visits_scaled_procedure'] }}`(
    "{{ params['project'] }}",
    "{{ params['metrics_dataset'] }}",
    "{{ params['day_stats_visits_scaled_table'] }}",
    "{{ params['poi_visits_scaled_dataset'] }}",
    "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}"
)