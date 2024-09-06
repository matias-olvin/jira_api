delete from 
    `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['smc_gtvm_metric_table'] }}`
where 
    update_date = '{{ ds }}' and
    step = "{{ dag_run.conf['step'] }}"