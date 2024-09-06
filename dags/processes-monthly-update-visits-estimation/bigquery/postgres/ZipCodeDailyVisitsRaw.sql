create or replace table
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['ZipCodeDailyVisitsRaw_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.adjustments_output`
    partition by local_date
    cluster by fk_zipcodes
as




WITH
  adjustments_output AS (
  SELECT
    fk_zipcodes, local_date, sum(visits) as visits
  FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['adjustments_output_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.adjustments_output`
  inner join
    (select pid as fk_sgplaces, cast(postal_code as int64) as fk_zipcodes from
    -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
    `{{ var.value.prod_project }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`

    ) using (fk_sgplaces)
  where local_date > '2018-12-31'
  group by fk_zipcodes, local_date
  
  ),
  -- picking observed or estimated based on activity
daily_visits_array AS (
  SELECT
    fk_zipcodes,
    local_date,
    TO_JSON_STRING(ARRAY_AGG( IFNULL(visits,
          0)
      ORDER BY
        local_date_sort )) AS visits
  FROM (
    SELECT
      fk_zipcodes,
      DATE_TRUNC(local_date, month) AS local_date,
      local_date AS local_date_sort,
      visits
    FROM
      adjustments_output)
  GROUP BY
    fk_zipcodes,
    local_date
),


tests as (
  select *
  from daily_visits_array
  , (select count(*) as count_distinct from (select distinct fk_zipcodes, local_date from daily_visits_array))
  , (select count(*) as count_ from (select  fk_zipcodes, local_date from daily_visits_array))
  , (
      select count(*) count_less_28
      from
      (SELECT LENGTH(visits) - LENGTH(REPLACE(visits, ',', '')) as comma_count, count(*) as ct
      FROM daily_visits_array
      where local_date < CURRENT_DATE()
      group by 1)
      where ct < 28
  )

)


select * except(count_distinct, count_, count_less_28)
from tests
WHERE IF(
(count_distinct = count_) AND count_less_28 = 0,
TRUE,
ERROR(FORMAT("count_distinct  %d <> count_ %d or count_less_28 > 0 %d ", count_distinct, count_, count_less_28))
) 

;

create or replace table
-- `storage-dev-olvin-com.visits_estimation.quality_activity_zipcode`
`{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['quality_activity_zipcode_table'] }}`

as
(
SELECT pid as fk_zipcodes, ifnull(activity_level, 17) as activity_level
from 
-- `storage-prod-olvin-com.postgres.ZipCodeRaw`
`{{ var.value.prod_project }}.{{ params['places_postgres_dataset'] }}.{{ params['zipcoderaw_table'] }}`
left join
(
SELECT distinct fk_zipcodes, 1 as activity_level
FROM  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['ZipCodeDailyVisitsRaw_table'] }}`
 
) on pid = fk_zipcodes
)