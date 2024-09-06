merge into
  `{{ var.value.env_project }}.{{ params['sns_metrics_dataset'] }}.{{ params['almanac_groups_table'] }}{{ dag_run.conf['step'] }}` all_metrics

using (
    with median_per_row as (
        select
            *,
            percentile_cont(cast({{ params.metric }} as float64), 0.5) over(
                partition by
                    {{ params.group_id }},
                    start_date,
                    end_date,
                    update_date
            ) AS median,
        from
            `{{ var.value.env_project }}.{{ params['sns_metrics_dataset'] }}.{{ params['almanac_pois_table'] }}{{ dag_run.conf['step'] }}`, 
            UNNEST({{ params.metric }}_{{ params.granularity }})
    ),
    join_results as (
        select
            {{ params.group_id }} as id,
            '{{ params.group_id }}' as group_id,
            update_date,
            start_date, 
            end_date, 
            any_value(median) as median, 
            stddev(cast({{ params.metric }} as float64)) as std_dev, 
            count(*) as total,
        from 
            median_per_row
        where
            update_date = '{{ ds }}'
        group by
            {{ params.group_id }},
            start_date,
            end_date,
            update_date
    )
    select
        id,
        ifnull(GetName(id), id) as name,
        group_id,
        update_date,
        ARRAY_AGG(
            struct(
                start_date, 
                end_date, 
                median, 
                std_dev, 
                total
            )
        ) as {{ params.metric }}_{{ params.granularity }}
    from join_results
    group by 
        id, 
        name, 
        group_id, 
        update_date
) latest_metrics

on 
    all_metrics.id = latest_metrics.id and 
    all_metrics.group_id = latest_metrics.group_id and
    all_metrics.update_date = latest_metrics.update_date

when matched then
  update set
    {{ params.metric }}_{{ params.granularity }} = latest_metrics.{{ params.metric }}_{{ params.granularity }},
    name = latest_metrics.name

when not matched then
    insert (
        id,			
        name,			
        group_id,			
        update_date,			
        {{ params.metric }}_{{ params.granularity }}
    )
    values (
        latest_metrics.id,
        latest_metrics.name,
        latest_metrics.group_id,
        latest_metrics.update_date,
        latest_metrics.{{ params.metric }}_{{ params.granularity }}
    )


