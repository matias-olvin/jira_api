create or replace table
--   `storage-dev-olvin-com.visits_estimation.adjustments_volume_output`
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_volume_output_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
as

with
input_block as
(
  select *
  from
    -- `storage-dev-olvin-com.visits_estimation.adjustments_covid_output` 
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_covid_output_table'] }}`
),

factors as
(
  select
  fk_sgplaces, ifnull(avg_output_visits/ nullif(avg_input_visits, 0), 1) as factor
from
  (
  select
    fk_sgplaces,
    input_avg.avg_input_visits,
    gtvm.visits as avg_output_visits
  from
  (
    select
      fk_sgplaces,
      avg( if ( (local_date >= '2020-03-01') and (local_date < '2020-09-01') , null, visits)  ) as avg_input_visits
    from input_block
    where local_date <= (
      select max(local_date)
      from
      `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.v-{{ params['poi_matching_dataset'] }}-{{ params['smc_production_dates_table'] }}`
    )
    group by fk_sgplaces
  ) as input_avg
  left join
    -- `storage-prod-olvin-com.ground_truth_volume_model.gtvm_output`
    `{{ var.value.prod_project }}.{{ params['gtvm_dataset'] }}.{{ params['gtvm_model_output_table'] }}`
    as gtvm
  using (fk_sgplaces)
  )
),

volume_scaled_visits as
(
  select
  fk_sgplaces, local_date, ifnull(factor, 1) * visits as visits
  from input_block
  left join factors
  using(fk_sgplaces)
),

tests as (
  select *
  from volume_scaled_visits
  , (select count(*) as count_in from input_block)
  , (select count(*) as count_out from volume_scaled_visits)

)

select * except(count_in, count_out)
from tests
WHERE IF(
count_in = count_out,
TRUE,
ERROR(FORMAT("count_in  %d > count_out %d ", count_in, count_out))
) 

