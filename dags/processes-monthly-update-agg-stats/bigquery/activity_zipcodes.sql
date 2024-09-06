INSERT
--    `storage-prod-olvin-com.visitors_home.final_activity_zipcode`
    `{{ var.value.env_project }}.{{ params['Activity_dataset'] }}.{{ params['ZipCode_activity_table'] }}`
select
  fk_zipcodes,
  -- DATE('2024-03-01') as run_date,
  CAST(DATE_TRUNC(CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE) , MONTH) AS DATE) as run_date,
  case
    when active_col = True then 'active'
--    when active_col is null and limited_act_col = True then 'inactive'
--    else 'no_data'
    else 'inactive'
  end
  as activity,
  case
    when active_col = True then 1
--    when active_col is null and limited_act_col = True then 0.47
    else 0.47
  end
  as confidence_level

from
  (select pid as fk_zipcodes from
--        `storage-prod-olvin-com.postgres.ZipCodeRaw`
        `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['ZipCodeRaw_table'] }}`
    )
left join
  (
    SELECT DISTINCT fk_zipcodes, True active_col FROM(
      SELECT CAST(postal_code AS INT64) AS fk_zipcodes
      FROM
--          `storage-prod-olvin-com.postgres.SGPlaceRaw`
          `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['PlaceRaw_table'] }}`
      WHERE pid IN(
        SELECT DISTINCT fk_sgplaces
        FROM
--            `storage-prod-olvin-com.postgres.SGPlaceDailyVisitsRaw`
        `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['PlaceRaw_daily_table'] }}`
      )
    )
  )
using (fk_zipcodes)
left join
  (
    SELECT DISTINCT fk_zipcodes, True limited_act_col FROM(
      SELECT CAST(postal_code AS INT64) AS fk_zipcodes
      FROM
--          `storage-prod-olvin-com.postgres.SGPlaceRaw`
          `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['PlaceRaw_table'] }}`
      WHERE pid IN(
        SELECT DISTINCT fk_sgplaces
        FROM
--            `storage-prod-olvin-com.postgres.SGPlaceMonthlyVisitsRaw`
        `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['PlaceRaw_monthly_table'] }}`
      )
    )
  )
using (fk_zipcodes)
