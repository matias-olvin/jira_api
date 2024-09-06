merge into
  `{{ var.value.env_project }}.{{ params['sns_metrics_dataset'] }}.{{ params['almanac_groups_table'] }}{{ dag_run.conf['step'] }}` all_metrics

using (
    select 
        id, 
        ifnull(GetName(id), id) as name,
        group_id, 
        update_date, 
        struct(
            cast(split(result, ',')[offset(0)] as float64) as discrepancy_10_pos,
            cast(split(result, ',')[offset(1)] as float64) as discrepancy_10_neg,
            cast(split(result, ',')[offset(2)] as float64) as discrepancy_50_pos,
            cast(split(result, ',')[offset(3)] as float64) as discrepancy_50_neg,
            cast(split(result, ',')[offset(4)] as float64) as discrepancy_90_pos,
            cast(split(result, ',')[offset(5)] as float64) as discrepancy_90_neg
        ) as {{ params.metric }}
    from  
        `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['almanac_accuracy_table'] }}{{ dag_run.conf['step'] }}_{{ ds_nodash }}` 
    where 
        metric = "{{ params.metric }}"
) latest_metrics

on 
    all_metrics.id = latest_metrics.id and
    all_metrics.group_id = latest_metrics.group_id and
    all_metrics.update_date = latest_metrics.update_date

when matched then 
    update set
        name = latest_metrics.name,
        {{ params.metric }} = latest_metrics.{{ params.metric }}

when not matched then
    insert (
        id,
        name,
        group_id,
        update_date,
        {{ params.metric }}
    )
    values (
        latest_metrics.id,
        latest_metrics.name,
        latest_metrics.group_id,
        latest_metrics.update_date,
        latest_metrics.{{ params.metric }}
    )