DELETE
    `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['tamoco_latency_table'] }}`
WHERE 
    provider_date = DATE("{{ ds }}")
