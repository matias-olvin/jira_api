BEGIN CREATE TEMP TABLE temp_covid_coll as
WITH
covid_collection as (
      select * from
        -- `storage-prod-olvin-com.regressors.covid`
        `{{ var.value.prod_project }}.{{ params['regressors_dataset'] }}.{{ params['covid_collection_table'] }}`
  )
select distinct local_date, factor,identifier from covid_collection;
end;

create or replace table
-- `storage-dev-olvin-com.visits_estimation.adjustments_covid_output`
`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_covid_output_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
as

with 
covid_dictionary as (
      select * from
        -- `storage-prod-olvin-com.regressors.covid_dictionary`
        `{{ var.value.prod_project }}.{{ params['regressors_dataset'] }}.{{ params['covid_dictionary_table'] }}`
  ),

adjustments_closings_output as (
  select *
  from
    -- `storage-dev-olvin-com.visits_estimation.adjustments_closings_output`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_closings_output'] }}`

),

template_covid as (
  select adjustments_closings_output.*, 
      county_restrictions_factor,
      city_restrictions_factor,
      state_restrictions_factor,
  from adjustments_closings_output
  inner join covid_dictionary using (
        fk_sgplaces
        )
  left join (select local_date, factor as county_restrictions_factor, identifier as identifier_county_restrictions  from temp_covid_coll)  using (identifier_county_restrictions, local_date)
  left join (select local_date, factor as city_restrictions_factor, identifier as identifier_city_restrictions  from temp_covid_coll)  using (identifier_city_restrictions, local_date)
  left join (select local_date, factor as state_restrictions_factor, identifier as identifier_state_restrictions  from temp_covid_coll)  using (identifier_state_restrictions, local_date)
),
covid_restrictions_added as (
    select 
        * except (restrictions_factor),
        1 - (restrictions_factor * restrictions_factor / 25) as restrictions_factor 
    from (SELECT
    * except(county_restrictions_factor, city_restrictions_factor, state_restrictions_factor),
    ifnull( ifnull( ifnull( city_restrictions_factor, county_restrictions_factor), state_restrictions_factor), 0) as restrictions_factor
    FROM
    template_covid)
),

output_adjustments_covid as 
(
  select 
    fk_sgplaces, local_date, ifnull(restrictions_factor, 1) * visits as visits
  from
    covid_restrictions_added
),

tests as (
  select *
  from output_adjustments_covid
  , (select count(*) as count_in from adjustments_closings_output)
  , (select count(*) as count_out from output_adjustments_covid)
  , (select CAST(sum(visits) as INT64) as sum_post_in from adjustments_closings_output where local_date = '2023-04-01')
  , (select CAST(sum(visits) as INT64) as sum_post_out from output_adjustments_covid where local_date = '2023-04-01')
  , (select CAST(sum(visits) as INT64) as sum_pre_in from adjustments_closings_output where local_date = '2019-12-01')
  , (select CAST(sum(visits) as INT64) as sum_pre_out from output_adjustments_covid where local_date = '2019-12-01')
  , (select CAST(sum(visits) as INT64) as sum_in from adjustments_closings_output where local_date = '2020-04-15')
  , (select CAST(sum(visits) as INT64) as sum_out from output_adjustments_covid where local_date = '2020-04-15')
)


select * except(count_in, count_out, sum_post_in, sum_post_out, sum_pre_in, sum_pre_out, sum_in, sum_out)
from tests
WHERE IF(
(count_in = count_out) AND (sum_post_in = sum_post_out) AND (sum_pre_in = sum_pre_out) AND (sum_in > sum_out),
TRUE,
ERROR(FORMAT("count_in  %d <> count_out %d . or sum_out_post %d <> sum_in_post %d . or sum_in_post %d <> sum_in_post %d . or sum_out %d >= sum_in %d  ", count_in, count_out, sum_post_in, sum_post_out, sum_pre_in, sum_pre_out, sum_in, sum_out))
) 