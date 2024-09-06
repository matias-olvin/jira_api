WITH

daily_visits as (
  SELECT
  DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
  CAST(visits as int64) visits_final,
  fk_zipcodes,
  -- row_number,

FROM
  (
    SELECT
    local_date,
      fk_zipcodes,
      JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
      `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ZipCodeDailyVisitsRaw_table'] }}`
    
  )
CROSS JOIN 
  UNNEST(visit_array) AS visits -- Convert array elements to row
  WITH OFFSET AS row_number -- Get the position in the array as another column
ORDER BY local_date, fk_zipcodes, row_number

),
  
  adding_zipcode_information as (
    select fk_zipcodes, local_date, visits_final
    from daily_visits 
    left join
      (
        select 
          pid as fk_zipcodes, primary_city, county,state
        from
          `{{ var.value.env_project }}.{{ params['zipcodes_dataset'] }}.{{ params['zipcodes_table'] }}`
          -- `storage-prod-olvin-com.sg_places.20211101`
      ) using (fk_zipcodes)
  )

SELECT
  *,
  NULL AS visits_estimated_lower,
  NULL AS visits_observed,
  NULL AS visits_estimated_upper,
  NULL AS visits_estimated,
  'adjustments' as step,
  'all' as mode,
  DATE('{{ ds }}') as run_date
FROM
  adding_zipcode_information