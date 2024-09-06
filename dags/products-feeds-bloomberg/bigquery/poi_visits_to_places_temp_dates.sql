WITH
  group_temp AS (
  SELECT DISTINCT local_date as local_date
    FROM 
      `{{ params['project'] }}.{{ params['bloomberg_dataset'] }}.temp_{{ ds }}`
  )
  select local_date
  from group_temp