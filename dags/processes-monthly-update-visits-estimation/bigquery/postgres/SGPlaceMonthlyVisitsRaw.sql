BEGIN CREATE TEMP TABLE monthly_adjustments_output as
select fk_sgplaces, date_trunc(local_date, month) as local_date, sum(visits) as visits
from
    -- `storage-dev-olvin-com.visits_estimation.adjustments_output`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_output_table'] }}`

group by 1,2
;end;

create or replace table
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
    partition by local_date
    cluster by fk_sgplaces
as

with monthly_scaled_visits as (

  select *
  from 
    (
      select 
      fk_sgplaces,
      tier_id_sns as tier_id
      from
      -- `storage-dev-olvin-com.visits_estimation.poi_class`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}`
      where fk_sgplaces not in (select distinct fk_sgplaces
                        from  monthly_adjustments_output
                        )
    )
  inner join 
  -- `storage-dev-olvin-com.visits_estimation.monthly_visits_class`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['monthly_visits_class_table'] }}`
  using (tier_id)

),


-- monthly visits where we did the gtvm
adding_avg_visits_and_target as (
    SELECT
        *
    from
        monthly_scaled_visits
    inner join  
      (
        select fk_sgplaces, avg(scaled_visits) as avg_scaled_visits
        from monthly_scaled_visits
        where  ((local_date > '2020-09-30') OR (local_date < '2020-03-01'))
        group by fk_sgplaces
      ) using (fk_sgplaces)
    inner join  
      (
        select fk_sgplaces, 365/12 * visits as target_visits_monthly_visits
        from
            -- `storage-prod-olvin-com.ground_truth_volume_model.gtvm_output`
            `{{ var.value.prod_project }}.{{ params['gtvm_dataset'] }}.{{ params['gtvm_model_output_table'] }}`
 
      ) using (fk_sgplaces)  

),

tests as (
  select *
  from adding_avg_visits_and_target
  , (select count(*) count_ from adding_avg_visits_and_target)
  , ( select count(*) count_distinct from (select distinct fk_sgplaces, local_date from adding_avg_visits_and_target) )
),

monthly_inactive_visits_base as (
  select * except(count_, count_distinct)
  from tests
  WHERE IF(
  (count_ = count_distinct),
  TRUE,
  ERROR(FORMAT("no consistency count_distinct <> count,  %d <> %d ", count_distinct, count_))
  )

),

test_volume as (
  select *
  from
  (select *
  from
    (select cast(avg(visits) as int64) avg_volume_active from monthly_adjustments_output  ),
    (select cast(avg(scaled_visits/avg_scaled_visits * target_visits_monthly_visits) as int64) avg_volume_limited_data from monthly_inactive_visits_base))
    WHERE IF(
    (avg_volume_active >  5*avg_volume_limited_data) or  (avg_volume_active <  0.2 *avg_volume_limited_data),
    TRUE,
    ERROR(FORMAT("no consistency avg_volume_active <> avg_volume_limited_data,  %d <> %d ", avg_volume_active, avg_volume_limited_data))
    )

),

monthly_visits_table as (
  select fk_sgplaces, local_date, CAST(scaled_visits/avg_scaled_visits * target_visits_monthly_visits as int64) as visits
  from monthly_inactive_visits_base

  union all 
  select fk_sgplaces, local_date, visits
  from monthly_adjustments_output
),

tests_2 as (
  select *
  from monthly_visits_table
  , (select count(*) count_ from adding_avg_visits_and_target)
  , ( select count(*) count_distinct from (select distinct fk_sgplaces, local_date from adding_avg_visits_and_target) )
),

SGPlaceMonthlyVisitsRaw as (
  select fk_sgplaces, local_date, cast(visits as int64) as visits
  from tests_2
  WHERE IF(
  (count_ = count_distinct),
  TRUE,
  ERROR(FORMAT("no consistency count_distinct <> count,  %d <> %d ", count_distinct, count_))
  )

)

select *
from SGPlaceMonthlyVisitsRaw





