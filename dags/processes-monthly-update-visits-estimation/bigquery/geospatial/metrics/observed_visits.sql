SELECT local_date, sum(visits_observed) as vo, DATE('{{ ds }}') as run_date
FROM  
    `{{ var.value.env_project }}.{{ visits_aggregated_dataset }}.{{ visits_aggregated_table }}`
group by local_date