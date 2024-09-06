delete from 
    `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['groups_table'] }}_{{ dag_run.conf['step'] }}`
where 
    update_date = '{{ ds }}'