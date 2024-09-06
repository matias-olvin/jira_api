create or replace table
    -- `storage-prod-olvin-com.visits_estimation.SGPlaceRanking`
    `{{ var.value.env_project }}.{{  params['visits_estimation_dataset'] }}.{{ params['SGPlaceRanking_table'] }}`
as

with

monthly_quarter_yearly_visits as (
  select
    fk_sgplaces,
    sum(if( (local_date <= (DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH))) and (local_date >= DATE_SUB((DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH)), INTERVAL 0 MONTH)), visits, null) ) as last_month_visits,
    sum(if( (local_date <= (DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH))) and (local_date >= DATE_SUB((DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH)), INTERVAL 2 MONTH)), visits, null) ) as last_quarter_visits,
    sum(if( (local_date <= (DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH))) and (local_date >= DATE_SUB((DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH)), INTERVAL 11 MONTH)), visits, null) ) as last_year_visits,
  from
    -- `storage-prod-olvin-com.visits_estimation.SGPlaceMonthlyVisitsRaw`
    `{{ var.value.env_project }}.{{  params['visits_estimation_dataset'] }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
  group by fk_sgplaces
  HAVING last_month_visits > 0 AND last_quarter_visits > 0 AND last_year_visits > 0



),

adding_region_cbsa_brand as (

  select *
  from monthly_quarter_yearly_visits
  inner join (
                select pid as fk_sgplaces, region, fk_sgbrands
                from
                    -- `storage-prod-olvin-com.sg_places.20211101`
                    `{{ var.value.prod_project }}.{{  params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
            )
  using (fk_sgplaces)
  LEFT JOIN(
    SELECT pid AS fk_sgplaces, cbsa_fips_code
    FROM
        -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
        `{{ var.value.prod_project }}.{{  params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    INNER JOIN
        -- `storage-prod-olvin-com.area_geometries.geo_us_boundaries-cbsa`
        `{{ var.value.prod_project }}.{{  params['area_geometries_dataset'] }}.{{ params['cbsa_table'] }}`
    ON ST_WITHIN(ST_GEOGPOINT(longitude, latitude), cbsa_geom)
  )
  using (fk_sgplaces)

),

adding_ranking as (

  select
    DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH) as local_date,
    fk_sgplaces,
    last_month_visits,
    row_number() over ( partition by fk_sgbrands order by last_month_visits desc  ) as nat_rank_last_month,
    row_number() over ( partition by fk_sgbrands, region order by last_month_visits desc  ) as state_rank_last_month,
    IF(cbsa_fips_code IS NULL, NULL, row_number() over ( partition by fk_sgbrands, cbsa_fips_code order by last_month_visits desc  ) ) as CBSA_rank_last_month,
    last_quarter_visits,
    row_number() over ( partition by fk_sgbrands order by last_quarter_visits desc  ) as nat_rank_last_quarter,
    row_number() over ( partition by fk_sgbrands, region order by last_quarter_visits desc  ) as state_rank_last_quarter,
    IF(cbsa_fips_code IS NULL, NULL, row_number() over ( partition by fk_sgbrands, cbsa_fips_code order by last_quarter_visits desc  ) ) as CBSA_rank_last_quarter,
    last_year_visits,
    row_number() over ( partition by fk_sgbrands order by last_year_visits desc  ) as nat_rank_last_year,
    row_number() over ( partition by fk_sgbrands, region order by last_year_visits desc  ) as state_rank_last_year,
    IF(cbsa_fips_code IS NULL, NULL, row_number() over ( partition by fk_sgbrands, cbsa_fips_code order by last_year_visits desc  ) ) as CBSA_rank_last_year,
    count(*) over (partition by fk_sgbrands) as total_nat,
    count(*) over (partition by fk_sgbrands, region) as total_state,
    count(*) over (partition by fk_sgbrands, cbsa_fips_code) as total_CBSA,
    
  from adding_region_cbsa_brand

),

adding_percentile as 

(
select 
  local_date,
  fk_sgplaces,
  CAST(last_month_visits AS int64) as last_month_visits,
  TO_JSON_STRING([nat_rank_last_month,total_nat]) as nat_rank_last_month,
  cast(100 * (1 - ((2*nat_rank_last_month - 1) / (2*total_nat))) as int64) as nat_perc_rank_last_month,
  TO_JSON_STRING([state_rank_last_month,total_state]) as state_rank_last_month,
  cast(100 * (1 - ((2*state_rank_last_month - 1) / (2*total_state))) as int64) as state_perc_rank_last_month,
  IF(CBSA_rank_last_month IS NULL, NULL, TO_JSON_STRING([CBSA_rank_last_month,total_CBSA])) as CBSA_rank_last_month,
  cast(100 * (1 - ((2*CBSA_rank_last_month - 1) / (2*total_CBSA))) as int64) as CBSA_perc_rank_last_month,

  CAST(last_quarter_visits AS int64) as last_quarter_visits,
  TO_JSON_STRING([nat_rank_last_quarter,total_nat]) as nat_rank_last_quarter,
  cast(100 * (1 - ((2*nat_rank_last_quarter - 1) / (2*total_nat))) as int64) as nat_perc_rank_last_quarter,
  TO_JSON_STRING([state_rank_last_quarter,total_state]) as state_rank_last_quarter,
  cast(100 * (1 - ((2*state_rank_last_quarter - 1) / (2*total_state))) as int64) as state_perc_rank_last_quarter,
  IF(CBSA_rank_last_quarter IS NULL, NULL, TO_JSON_STRING([CBSA_rank_last_quarter,total_CBSA])) as CBSA_rank_last_quarter,
  cast(100 * (1 - ((2*CBSA_rank_last_quarter - 1) / (2*total_CBSA))) as int64) as CBSA_perc_rank_last_quarter,

  CAST(last_year_visits AS int64) as last_year_visits,
  TO_JSON_STRING([nat_rank_last_year,total_nat]) as nat_rank_last_year,
  cast(100 * (1 - ((2*nat_rank_last_year - 1) / (2*total_nat))) as int64) as nat_perc_rank_last_year,
  TO_JSON_STRING([state_rank_last_year,total_state]) as state_rank_last_year,
  cast(100 * (1 - ((2*state_rank_last_year - 1) / (2*total_state))) as int64) as state_perc_rank_last_year,
  IF(CBSA_rank_last_year IS NULL, NULL, TO_JSON_STRING([CBSA_rank_last_year,total_CBSA])) as CBSA_rank_last_year,
  cast(100 * (1 - ((2*CBSA_rank_last_year - 1) / (2*total_CBSA))) as int64) as CBSA_perc_rank_last_year,
  


from 

  adding_ranking

)

select *
from adding_percentile


