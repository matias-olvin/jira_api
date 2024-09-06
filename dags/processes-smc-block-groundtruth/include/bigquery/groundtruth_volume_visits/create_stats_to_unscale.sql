CREATE OR REPLACE TABLE
-- sns-vendor-olvin-poc.gtvm_dev.stats_to_unscale_block_A
`{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['stats_to_unscale_block_A_table'] }}`
AS

with geospatial_visits as (
  select *
  from 
  (select pid as fk_sgplaces, fk_sgbrands, naics_code from
        -- `storage-prod-olvin-com.smc_postgres.SGPlaceRaw`
        `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
        where fk_sgbrands is not null)
  left join
    (
    SELECT
      fk_sgplaces,
      log(nullif(sum(visit_score) / (DATE_DIFF(MAX(local_date), MIN(local_date), DAY) + 1), 0)) as avg_log_visits_geospatial -- Hardcoded number of days removed from here
    FROM  `{{ var.value.env_project }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}.*`
    WHERE local_date > "2021-01-01"
    group by 1) using (fk_sgplaces)
),

adding_avg_stddev as (
  select 
    fk_sgbrands, any_value(naics_code) as naics_code, avg(avg_log_visits_geospatial) as avg_avg_log_visits_geospatial, stddev(avg_log_visits_geospatial) as stddev_avg_log_visits_geospatial 
  from
    geospatial_visits
  group by 1

),

adding_group_values as (
  select
    *,
    avg(avg_avg_log_visits_geospatial) over (partition by naics_code) avg_log_visits_geospatial_naics_code,
    avg(avg_avg_log_visits_geospatial) over () avg_log_visits_geospatial_all,
    avg(stddev_avg_log_visits_geospatial) over (partition by naics_code) stddev_log_visits_geospatial_naics_code,
    avg(stddev_avg_log_visits_geospatial) over () stddev_log_visits_geospatial_all

  from adding_avg_stddev
),

biased_stddev as (
  select 
    fk_sgbrands,
    CASE 
    when (stddev_avg_log_visits_geospatial is null) and (stddev_log_visits_geospatial_naics_code is null) THEN avg_log_visits_geospatial_all
    when (stddev_avg_log_visits_geospatial is null) THEN avg_log_visits_geospatial_naics_code
    else avg_avg_log_visits_geospatial
    END AS
    brand_log_avg
  ,
    CASE 
    when (stddev_avg_log_visits_geospatial is null) and (stddev_log_visits_geospatial_naics_code is null) THEN stddev_log_visits_geospatial_all
    when (stddev_avg_log_visits_geospatial is null) THEN stddev_log_visits_geospatial_naics_code
    else stddev_avg_log_visits_geospatial
    END AS
    brand_log_stddev
  from 
    adding_group_values
),

unbiasing_factor_table as (
    select 
      distinct PERCENTILE_CONT(factor_unbias_stddev, 0.5) over () factor_unbias_stddev
    from
    (
    select *, brand_stddev_gt / brand_log_stddev as factor_unbias_stddev
    from
    (
      select   fk_sgbrands, stddev(log(visits_per_day+1)) as brand_stddev_gt
      from
        `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.v-{{ params['smc_gtvm_dataset'] }}-{{ params['gtvm_target_agg_sns_table'] }}`

      inner join (select pid as fk_sgplaces, fk_sgbrands, naics_code from
        -- `storage-prod-olvin-com.smc_postgres.SGPlaceRaw`
        `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
        ) using (fk_sgplaces)
      group by 1
    )
    inner join 
      biased_stddev
    using (fk_sgbrands)
    where brand_stddev_gt is not null
    )
)

select fk_sgbrands, brand_log_avg, brand_log_stddev * factor_unbias_stddev as brand_log_stddev
from biased_stddev, unbiasing_factor_table

