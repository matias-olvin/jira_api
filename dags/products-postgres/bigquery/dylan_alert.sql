DECLARE metrics_table string DEFAULT 'storage-prod-olvin-com.postgres_metrics.trend';

CREATE OR REPLACE TABLE `storage-prod-olvin-com.postgres_metrics.dylans_table`
AS
with daily_correlation as (
  select
    distinct
      class_name as fk_sgbrands,
      PERCENTILE_CONT(correlation[SAFE_OFFSET(0)].poi_correlation, 0.5) over (partition  by class_name) as median_correlation_daily,
      count(*) over (partition  by class_name) as count_with_ground_truth
  from (
      select *
      from
        `storage-prod-olvin-com.postgres_metrics.trend`
      where
        run_date = "{{ ds }}"
        and class_id = 'fk_sgbrands'
        and correlation[SAFE_OFFSET(0)].days = 91
        and granularity = 'day'
        and pipeline = 'postgres'
        and step = 'final'
    )
),
weekly_correlation as (
  select
    distinct
      class_name as fk_sgbrands,
      PERCENTILE_CONT(correlation[SAFE_OFFSET(0)].poi_correlation, 0.5) over (partition  by class_name) as median_correlation_weekly,
  from (
      select *
      from
        `storage-prod-olvin-com.postgres_metrics.trend`
      where
        run_date = "{{ ds }}"
        and class_id = 'fk_sgbrands'
        and correlation[SAFE_OFFSET(0)].days = 182
        and granularity = 'week'
        and pipeline = 'postgres'
        and step = 'final'
    )
),
monthly_correlation as (
  select
    distinct
      class_name as fk_sgbrands,
      PERCENTILE_CONT(correlation[SAFE_OFFSET(0)].poi_correlation, 0.5) over (partition  by class_name) as median_correlation_monthly,
  from (
      select *
      from
        `storage-prod-olvin-com.postgres_metrics.trend`
      where
        run_date = "{{ ds }}"
        and class_id = 'fk_sgbrands'
        and correlation[SAFE_OFFSET(0)].days = 1642
        and granularity = 'month'
        and pipeline = 'postgres'
        and step = 'final'
    )
),
total_active_poi_per_brand as (
  select 
    count(distinct fk_sgplaces) as count_total,
    fk_sgbrands
  from 
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.SGPlaceDailyVisitsRaw`
  inner join 
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.SGPlaceRaw` on pid=fk_sgplaces
  group by fk_sgbrands

),
all_candidates as (
  select *
  from 
    total_active_poi_per_brand
  inner join daily_correlation using (fk_sgbrands)
  inner join weekly_correlation using (fk_sgbrands)
  inner join monthly_correlation using (fk_sgbrands)
  inner join ( 
    select 
      name, 
      pid as fk_sgbrands 
    from 
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.SGBrandRaw`
  ) 
      using (fk_sgbrands)
)

select
  * except(fk_sgbrands),
  count_with_ground_truth / count_total as ratio_pois_used_in_validation
from all_candidates
where 
  count_with_ground_truth / count_total >= 0.05
  and count_total > 50
  and median_correlation_daily >= 0.7