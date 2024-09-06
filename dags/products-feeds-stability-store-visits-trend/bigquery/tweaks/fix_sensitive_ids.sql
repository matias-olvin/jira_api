DELETE FROM 
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_trend_temp_table'] }}`
WHERE store_id NOT IN (
  SELECT pid 
  FROM 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['places_dataset'] }}-{{ params['places_dynamic_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
  WHERE naics_code IN (
    SELECT naics_code 
    FROM 
        `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['base_tables_dataset'] }}-{{ params['non_sensitive_naics_codes_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
  )
)
