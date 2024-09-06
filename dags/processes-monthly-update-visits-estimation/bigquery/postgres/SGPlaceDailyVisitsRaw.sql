create or replace table
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['SGPlaceDailyVisitsRaw_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.adjustments_output`
    partition by local_date
    cluster by fk_sgplaces
as




WITH
  adjustments_output AS (
  SELECT
    fk_sgplaces, local_date, cast(visits as int64) as visits
  FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['adjustments_output_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.adjustments_output`
  where local_date > '2018-12-31'
  
  ),
  -- picking observed or estimated based on activity
daily_visits_array AS (
  SELECT
    fk_sgplaces,
    local_date,
    TO_JSON_STRING(ARRAY_AGG( IFNULL(visits,
          0)
      ORDER BY
        local_date_sort )) AS visits
  FROM (
    SELECT
      fk_sgplaces,
      DATE_TRUNC(local_date, month) AS local_date,
      local_date AS local_date_sort,
      visits
    FROM
      adjustments_output)
  GROUP BY
    fk_sgplaces,
    local_date
),


tests as (
  select *
  from daily_visits_array
  , (select count(*) as count_distinct from (select distinct fk_sgplaces, local_date from daily_visits_array))
  , (select count(*) as count_ from (select  fk_sgplaces, local_date from daily_visits_array))
  , (
      select count(*) count_less_28
      from
      (SELECT LENGTH(visits) - LENGTH(REPLACE(visits, ',', '')) as comma_count, count(*) as ct
      FROM daily_visits_array
      where local_date < CURRENT_DATE()
      group by 1)
      where comma_count < 27
  )

)


select * except(count_distinct, count_, count_less_28)
from tests
WHERE IF(
(count_distinct = count_) AND count_less_28 = 0,
TRUE,
ERROR(FORMAT("count_distinct  %d <> count_ %d or count_less_28 >0 %d ", count_distinct, count_, count_less_28))
) 
