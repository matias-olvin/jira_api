DELETE FROM 
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_trend_temp_table'] }}`
WHERE store_id IN (
  SELECT DISTINCT store_id 
  FROM 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_trend_temp_table'] }}`
  JOIN (
    SELECT 
      street_address
      , name
      , zip_code
    FROM (
      SELECT DISTINCT
        store_id
        , street_address
        , name
        , zip_code
      FROM 
        `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_trend_temp_table'] }}` 
    )
    GROUP BY 
      street_address
      , name
      , zip_code
    HAVING COUNT(store_id) > 1
  )
  USING(name, street_address, zip_code)
)
