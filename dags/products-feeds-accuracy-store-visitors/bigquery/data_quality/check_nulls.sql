SELECT COUNT(*)
FROM (
  SELECT col_name, COUNT(1) nulls_count
  FROM 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visitors_temp_table'] }}` t,
  UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(t), r'"(\w+)":null')) col_name
  GROUP BY col_name
)
