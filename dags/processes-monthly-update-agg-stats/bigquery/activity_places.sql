INSERT
    -- `storage-prod-olvin-com.visitors_home.final_activity`
    `{{ var.value.env_project }}.{{ params['Activity_dataset'] }}.{{ params['Activity_table'] }}`
select 
  fk_sgplaces,
  -- DATE('2023-07-01') as run_date,
  CAST(DATE_TRUNC(CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE) , MONTH) AS DATE) as run_date,
  case 
    when active_col = True then 'active'
    when active_col is null and limited_act_col = True then 'inactive'
    else 'no_data'
  end
  as activity,
  case 
    when active_col = True then 1
    when active_col is null and limited_act_col = True then 0.47
    else 0.47
  end
  as confidence_level,
  ifnull(home_location_data_col, false ) home_location_data,
  ifnull(demographics_data_col, false ) demographics_data,
  true as networks_data

from
  (select pid as fk_sgplaces from
        -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
        `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['PlaceRaw_table'] }}`

    )
left join 
  (select distinct fk_sgplaces, True active_col from
        -- `storage-prod-olvin-com.postgres.SGPlaceDailyVisitsRaw`
        `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['PlaceRaw_daily_table'] }}`
        )
using (fk_sgplaces)
left join 
  (select distinct fk_sgplaces, True limited_act_col from
    --`storage-prod-olvin-com.postgres.SGPlaceMonthlyVisitsRaw`
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['PlaceRaw_monthly_table'] }}`
    )
using (fk_sgplaces)
left join (SELECT DISTINCT fk_sgplaces, True home_location_data_col  FROM
        -- `storage-prod-olvin-com.visitors_home.zipcode`
        `{{ var.value.env_project }}.{{ params['PlaceHome_dataset'] }}.{{ params['ZipCodeDemographic_table'] }}`
        ) zip_demos using (fk_sgplaces)
left join (SELECT DISTINCT fk_sgplaces, True demographics_data_col  FROM
    --`storage-prod-olvin-com.demographics.`
    --`storage-prod-olvin-com.postgres.SGPlaceCameoMonthlyRaw`
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplacecameomonthlyraw_table'] }}`
    WHERE local_date = DATE_TRUNC("{{ ds }}", MONTH)
    AND length(cameo_scores) >= 3
    ) demos using (fk_sgplaces)

