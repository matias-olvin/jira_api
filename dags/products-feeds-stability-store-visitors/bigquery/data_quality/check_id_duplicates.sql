WITH distinct_stores AS (
  SELECT COUNT(DISTINCT store_id) AS distinct_total
  FROM 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visitors_temp_table'] }}`
)
, grouped_stores AS (
  SELECT COUNT(*) AS grouped_total
  FROM (
    SELECT store_id
    FROM 
      `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visitors_temp_table'] }}`
    GROUP BY store_id
  )
)

SELECT grouped_total - distinct_total AS _count
FROM 
  grouped_stores
  , distinct_stores 
