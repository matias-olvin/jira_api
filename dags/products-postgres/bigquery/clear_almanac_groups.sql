delete from 
    `{{ var.value.env_project }}.{{ params['sns_metrics_dataset'] }}.{{ params['almanac_groups_table'] }}{{ dag_run.conf['step'] }}`
where 
    update_date = '{{ ds }}'