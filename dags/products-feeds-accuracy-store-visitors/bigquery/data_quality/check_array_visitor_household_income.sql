SELECT COUNT(*)
FROM (
  SELECT
    store_id
    , SUM(SAFE_CAST(REGEXP_REPLACE(_values, r'[^0-9.]', '') AS FLOAT64)) AS _sum
  FROM
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visitors_temp_table'] }}`,
    UNNEST(SPLIT(visitor_household_income,",")) _values
  GROUP BY store_id
)
WHERE 
  _sum < 99.95
  AND _sum > 100.05
