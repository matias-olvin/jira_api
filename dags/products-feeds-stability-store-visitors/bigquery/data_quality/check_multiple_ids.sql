SELECT COUNT(*) AS _count FROM (
  SELECT COUNT(*) AS _count
  FROM (
    SELECT DISTINCT
      store_id
      , street_address
      , name
      , zip_code
    FROM 
      `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visitors_temp_table'] }}` 
  )
  GROUP BY 
    street_address
    , name
    , zip_code
  HAVING COUNT(store_id) > 1
)
