create or replace table
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['SGPlaceHourlyVisitsRaw_table'] }}`
    -- `storage-dev-olvin-com.visits_estimation.SGPlaceHourlyVisitsRaw`
    partition by local_date
    cluster by fk_sgplaces
as



SELECT *
FROM
  -- `storage-dev-olvin-com.visits_estimation.SGPlaceHourlyAllVisitsRaw_table`
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{  params['SGPlaceHourlyAllVisitsRaw_table'] }}`
WHERE local_date >= DATE_TRUNC(DATE_SUB( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 0 MONTH), MONTH)
AND local_date <= DATE_ADD( CAST("{{ ds.format('%Y-%m-%d') }}" AS DATE), INTERVAL 5 MONTH)

