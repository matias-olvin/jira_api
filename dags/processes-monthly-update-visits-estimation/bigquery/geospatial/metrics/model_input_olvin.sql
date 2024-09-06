SELECT local_date,
    sum(y) as input_visits,
    AVG(christmas_factor) as christmas_factor,
    AVG(temperature_res) as temperature_res,
    AVG(temperature) as temperature,
    AVG(precip_intensity) as precip_intensity,
    AVG(group_factor) as group_factor,
    DATE('{{ ds }}') as run_date
FROM  
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['model_input_olvin_table'] }}`
group by local_date