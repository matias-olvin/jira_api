WITH sensitive_store_ids AS (
  SELECT pid AS store_id
  FROM 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['places_dataset'] }}-{{ params['places_dynamic_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
  WHERE naics_code NOT IN (
    SELECT naics_code
    FROM 
      `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['base_tables_dataset'] }}-{{ params['non_sensitive_naics_codes_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
  )
)

SELECT COUNT(*) as _count
FROM 
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_trend_temp_table'] }}` 
WHERE store_id IN (
  SELECT store_id 
  FROM sensitive_store_ids
)
