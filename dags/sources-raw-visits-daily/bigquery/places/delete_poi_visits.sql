DELETE 
    `{{ params['project'] }}.{{ params['poi_visits_dataset'] }}.{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}`
WHERE
    local_date = DATE_SUB("{{ ds }}", INTERVAL {{ var.value.latency_days_visits|int }} DAY)