create or replace table
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['SGPlaceHourlyAllVisitsRaw_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.SGPlaceHourlyAllVisitsRaw_table`
    partition by local_date
    cluster by fk_sgplaces
as



with adjustments_output_hourly as (
  SELECT     fk_sgplaces, local_date, local_hour, cast(visits as int64) as visits
  FROM
    -- `storage-dev-olvin-com.visits_estimation.adjustments_output_hourly`
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['adjustments_output_hourly_table'] }}`

),

hourly_visits_array as (

  SELECT
      fk_sgplaces, local_date,
      TO_JSON_STRING(ARRAY_AGG(
      IFNULL(visits, 0)
      ORDER BY
          local_hour
      )) AS visits
  FROM
    adjustments_output_hourly
  GROUP BY fk_sgplaces, local_date

),

tests as (
  select *
  from hourly_visits_array
  , (select count(*) as count_distinct from (select distinct fk_sgplaces, local_date from hourly_visits_array))
  , (select count(*) as count_ from (select  fk_sgplaces, local_date from hourly_visits_array))
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
