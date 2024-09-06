DELETE
    `{{ params['project'] }}.{{ params['poi_visits_scaled_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}` 
where 
    local_date = DATE_ADD( DATE('{{ ds }}'), INTERVAL - {{ var.value.latency_days_visits|int }} DAY )