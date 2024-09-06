CREATE TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['static_features_metrics_dataset'] }}.{{ params['dataforseo_table'] }}` (
  fk_sgbrands STRING
  , update_date DATE
  , coverage FLOAT64
  , num_added_last_update INTEGER
  , min_rating FLOAT64
  , perc10_rating FLOAT64
  , avg_rating FLOAT64
  , perc90_rating FLOAT64
  , max_rating FLOAT64
  , min_seo_count INTEGER
  , perc10_seo_count INTEGER
  , median_seo_count INTEGER
  , perc90_seo_count INTEGER
  , max_seo_count INTEGER
);

DELETE FROM `{{ var.value.env_project }}.{{ params['static_features_metrics_dataset'] }}.{{ params['dataforseo_table'] }}`
WHERE update_date = "{{ ds }}"
;

INSERT INTO `{{ var.value.env_project }}.{{ params['static_features_metrics_dataset'] }}.{{ params['dataforseo_table'] }}`
WITH updated_data AS (
  SELECT a.*
  FROM `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_history_table'] }}` a
  INNER JOIN (
    SELECT fk_sgplaces, MAX(update_date) AS update_date
    FROM `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_history_table'] }}`
    GROUP BY fk_sgplaces
  )
  USING (
    fk_sgplaces
    , update_date
  )
)
SELECT
  fk_sgbrands
  , DATE("{{ ds }}") AS update_date
  , COUNTIF(rating IS NOT NULL AND seo_count IS NOT NULL) / COUNT(*) AS coverage
  , COUNTIF(rating IS NOT NULL AND seo_count IS NOT NULL AND update_date = "{{ ds }}") AS num_added_last_update
  , MIN(rating) AS min_rating
  , APPROX_QUANTILES(rating, 10)[OFFSET(1)] AS perc10_rating
  , AVG(rating) AS avg_rating
  , APPROX_QUANTILES(rating, 10)[OFFSET(9)] AS perc90_rating
  , MAX(rating) AS max_rating
  , MIN(seo_count) AS min_seo_count
  , APPROX_QUANTILES(seo_count, 10)[OFFSET(1)] AS perc10_seo_count
  , APPROX_QUANTILES(seo_count, 2)[OFFSET(1)] AS median_seo_count
  , APPROX_QUANTILES(seo_count, 10)[OFFSET(9)] AS perc90_seo_count
  , MAX(seo_count) AS max_seo_count
FROM (
  SELECT pid AS fk_sgplaces, fk_sgbrands
  FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  WHERE fk_sgbrands IS NOT NULL
)
LEFT JOIN updated_data
USING(fk_sgplaces)
GROUP BY fk_sgbrands

UNION ALL


SELECT
  'all' AS fk_sgbrands
  , DATE("{{ ds }}") AS update_date
  , COUNTIF(rating IS NOT NULL AND seo_count IS NOT NULL) / COUNT(*) AS coverage
  , COUNTIF(rating IS NOT NULL AND seo_count IS NOT NULL AND update_date = "{{ ds }}") AS num_added_last_update
  , MIN(rating) AS min_rating
  , APPROX_QUANTILES(rating, 10)[OFFSET(1)] AS perc10_rating
  , AVG(rating) AS avg_rating
  , APPROX_QUANTILES(rating, 10)[OFFSET(9)] AS perc90_rating
  , MAX(rating) AS max_rating
  , MIN(seo_count) AS min_seo_count
  , APPROX_QUANTILES(seo_count, 10)[OFFSET(1)] AS perc10_seo_count
  , APPROX_QUANTILES(seo_count, 2)[OFFSET(1)] AS median_seo_count
  , APPROX_QUANTILES(seo_count, 10)[OFFSET(9)] AS perc90_seo_count
  , MAX(seo_count) AS max_seo_count
FROM (
  SELECT pid AS fk_sgplaces
  FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
)
LEFT JOIN updated_data
USING(fk_sgplaces)
;