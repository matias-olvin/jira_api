CALL `{{ var.value.env_project }}.{{ params['smc_day_stats_visits_scaled_procedure'] }}`(
    "{{ var.value.env_project }}",
    "{{ params['smc_metrics_dataset'] }}",
    "{{ params['day_stats_visits_scaled_table'] }}",
    "{{ params['poi_visits_scaled_dataset'] }}",
    "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}"
)