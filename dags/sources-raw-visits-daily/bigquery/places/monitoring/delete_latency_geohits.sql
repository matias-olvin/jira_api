DELETE
    `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['geohits_latency_table'] }}`
WHERE 
    provider_date = DATE("{{ ds }}")