BEGIN
  CREATE temp TABLE quality_output AS

with discarding_pois as (
  select distinct fk_sgplaces, previous
  from
    -- `storage-dev-olvin-com.visits_estimation.spurious_places`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['spurious_places_table'] }}`

),

this_month_healthy_visits as (

  select 
    fk_sgplaces, local_date, visits
  from
  (
  select *
  from 
    -- `storage-dev-olvin-com.visits_estimation.ground_truth_output`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_table'] }}`

  left join (select * from discarding_pois where previous = False) using (fk_sgplaces)
  )
  where previous is null

),

previous_month_healthy_visits as (
select fk_sgplaces, local_date, visits
from  

  (
  select *
  from 
    -- `storage-dev-olvin-com.visits_estimation.quality_output_previous`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_previous_table'] }}`
  left join (select * 
            from discarding_pois
            where previous = True 
            ) using(fk_sgplaces)
  )
  where previous is null and fk_sgplaces not in (select distinct fk_sgplaces from this_month_healthy_visits)

)

select *
from
  (
  select *
  from this_month_healthy_visits
  union all
  select *
  from  previous_month_healthy_visits
  )
inner join (select pid as fk_sgplaces from
    -- `storage-prod-olvin-com.postgres_final.SGPlaceRaw`
    `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
where fk_sgbrands is not null) using (fk_sgplaces)
inner join (select distinct fk_sgplaces from `{{ var.value.prod_project }}.{{ params['gtvm_dataset'] }}.{{ params['gtvm_model_output_table'] }}`)
  using (fk_sgplaces)
;end;


BEGIN
  CREATE temp TABLE quality_activity AS
select fk_sgplaces, ifnull(activity_level, 17) as activity_level
from
(
select pid as fk_sgplaces  
from
    -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
    `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
)
left join (
          select distinct fk_sgplaces, 1 as activity_level
          from quality_output
          inner join `{{ var.value.prod_project }}.{{ params['gtvm_dataset'] }}.{{ params['gtvm_model_output_table'] }}` using (fk_sgplaces)
          )
using (fk_sgplaces)
;end;


create or replace table
-- `storage-dev-olvin-com.visits_estimation.quality_output` 
`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
as
with 
tests as (
  select *

  from quality_output
  ,
  (
    select count(*) count_distinct from
    ( select distinct fk_sgplaces, local_date from quality_output)
  )
  ,
  (
    select count(*) count_ from
    ( select fk_sgplaces, local_date from quality_output)
  )
)

select *  except(count_, count_distinct)
from
  tests
WHERE IF(
count_ = count_distinct,
TRUE,
ERROR(FORMAT("count_  %d > count_distinct %d ", count_, count_distinct))
)

;


create or replace table
-- `storage-dev-olvin-com.visits_estimation.quality_activity` 
`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_activity_table'] }}`
as
with 
tests as (
  select *

  from quality_activity
  ,
  (
    select count(*) count_distinct from
    ( select distinct fk_sgplaces from quality_activity)
  )
  ,
  (
    select count(*) count_ from
    ( select fk_sgplaces from quality_activity)
  )
)

select *  except(count_, count_distinct)
from
  tests
WHERE IF(
count_ = count_distinct,
TRUE,
ERROR(FORMAT("count_  %d > count_distinct %d ", count_, count_distinct))
)

;

