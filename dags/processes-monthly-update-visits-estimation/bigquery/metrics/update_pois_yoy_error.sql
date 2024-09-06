merge into
  `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['pois_table'] }}_{{ dag_run.conf['step'] }}` all_metrics

using (
  with {{ params.metric }} as (
    select 
      id as fk_sgplaces,
      update_date,
      array_agg(
        struct(
          start_date, 
          end_date, 
          cast(split(result, ',')[offset(0)] as float64) as {{ params.metric }},
          cast(split(result, ',')[offset(1)] as float64) as {{ params.metric }}_abs
        ) 
      ) as results
    from 
      `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['gtvm_accuracy_table']}}_{{ ds_nodash }}` 
    where 
      metric = '{{ params.metric }}' and
      granularity = '{{ params.granularity }}' and
      step = '{{ dag_run.conf.step }}'
    group by 
      id, 
      update_date
  ),
  additional_info as (
    select
      pid as fk_sgplaces,
      fk_sgbrands,
      name,
      top_category,
      CAST(naics_code as string) as naics_code,
      region,
      street_address
    from
      `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  ),
  joining_tables as (
    select 
      fk_sgplaces,
      fk_sgbrands,
      name,
      top_category,
      naics_code,
      region,
      street_address,
      update_date,
      results as {{ params.metric }}_{{ params.granularity }}
    from
      {{ params.metric }}
    join
      additional_info
    using(fk_sgplaces)
  )
  select * from joining_tables
) latest_metrics

on 
  all_metrics.fk_sgplaces = latest_metrics.fk_sgplaces and 
  all_metrics.update_date = latest_metrics.update_date

when matched then
  update set
    {{ params.metric }}_{{ params.granularity }} = latest_metrics.{{ params.metric }}_{{ params.granularity }},
    fk_sgbrands = latest_metrics.fk_sgbrands,
    name = latest_metrics.name,
    top_category = latest_metrics.top_category,
    naics_code = latest_metrics.naics_code,
    region = latest_metrics.region,
    street_address = latest_metrics.street_address

when not matched then 
  insert (
    fk_sgplaces,
    fk_sgbrands,
    name,
    top_category,
    naics_code,
    region,
    street_address,
    update_date,
    {{ params.metric }}_{{ params.granularity }}
  )
  values (
    latest_metrics.fk_sgplaces,
    latest_metrics.fk_sgbrands,
    latest_metrics.name,
    latest_metrics.top_category,
    latest_metrics.naics_code,
    latest_metrics.region,
    latest_metrics.street_address,
    latest_metrics.update_date,
    latest_metrics.{{ params.metric }}_{{ params.granularity }}
  )