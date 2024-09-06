create or replace table
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['ZipCodeHourlyVisitsRaw_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.ZipCodeHourlyVisitsRaw`
    partition by local_date
    cluster by fk_zipcodes
as



with adjustments_output_hourly as (
    SELECT 
        fk_zipcodes, local_date, local_hour, sum(cast(visits as int64)) as visits
    FROM
        -- `storage-dev-olvin-com.visits_estimation.adjustments_output_hourly`
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['adjustments_output_hourly_table'] }}`
    inner join
        (select pid as fk_sgplaces, cast(postal_code as int64) as fk_zipcodes from
        -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
        `{{ var.value.prod_project }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`

        ) using (fk_sgplaces)

    WHERE local_date >= DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH)
    AND local_date <= DATE_ADD( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 5 MONTH)

    group by fk_zipcodes, local_date, local_hour


),

hourly_visits_array as (

  SELECT
      fk_zipcodes, local_date,
      TO_JSON_STRING(ARRAY_AGG(
      IFNULL(visits, 0)
      ORDER BY
          local_hour
      )) AS visits
  FROM
    adjustments_output_hourly
  GROUP BY fk_zipcodes, local_date

),

tests as (
  select *
  from hourly_visits_array
  , (select count(*) as count_distinct from (select distinct fk_zipcodes, local_date from hourly_visits_array))
  , (select count(*) as count_ from (select  fk_zipcodes, local_date from hourly_visits_array))
  , (
      select count(*) count_no_24
      from
      (SELECT LENGTH(visits) - LENGTH(REPLACE(visits, ',', '')) as comma_count, count(*) as ct
      FROM hourly_visits_array
      where local_date < CURRENT_DATE()
      group by 1)
      where comma_count <> 23
  )

)

select
  * except(count_distinct, count_, count_no_24)
from tests
WHERE IF(
(count_distinct = count_) AND count_no_24 = 0,
TRUE,
ERROR(FORMAT("count_distinct  %d <> count_ %d or count_no_24 <>0 : %d ", count_distinct, count_, count_no_24))
) 
