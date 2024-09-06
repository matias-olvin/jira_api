CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_daily_estimation_dataset'] }}.{{ params['grouped_daily_olvin_table'] }}` 
partition by date_trunc(local_date, MONTH)
cluster by local_date 
as
select
  sum(visit_score) as visits,
  local_date
from
  `{{ var.value.env_project }}.{{ params['poi_visits_block_1_dataset'] }}.*`
inner join
  `{{ var.value.env_project }}.{{ params['smc_daily_estimation_dataset'] }}.{{ params['poi_list_table'] }}`
  using (fk_sgplaces)
group by local_date