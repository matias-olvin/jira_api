INSERT INTO `{{ params['project'] }}.{{ params['daily_estimation_dataset'] }}.{{ params['grouped_daily_olvin_table'] }}` 
select
  sum(visit_score) as visits,
  local_date
from
  (select * from `{{ params['project'] }}.{{ params['poi_visits_staging_dataset'] }}.{{ params['block_1_output_table'] }}_{{ ds }}`
  where 
  local_date = "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}")
inner join
  `{{ params['project'] }}.{{ params['daily_estimation_dataset'] }}.{{ params['poi_list_table'] }}`
  using (fk_sgplaces)
group by local_date