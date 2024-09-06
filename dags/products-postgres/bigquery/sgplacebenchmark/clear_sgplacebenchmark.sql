delete from
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGPlaceBenchmarkingRaw_table'] }}`
where
    local_date >= '{{ ds }}'