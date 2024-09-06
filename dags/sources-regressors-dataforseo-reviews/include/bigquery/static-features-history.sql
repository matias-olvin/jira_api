CREATE TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_history_table'] }}` (
  fk_sgplaces STRING
  , update_date DATE
  , rating FLOAT64
  , seo_count INTEGER
);

DELETE FROM `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_history_table'] }}`
WHERE
  fk_sgplaces IN (
    SELECT fk_sgplaces
    FROM `{{ var.value.env_project }}.{{ params['static_features_staging_dataset'] }}.{{ params['dataforseo_table'] }}`
  ) AND update_date = "{{ ds }}"
;

INSERT INTO `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_history_table'] }}`
SELECT
  fk_sgplaces
  , DATE("{{ ds }}") AS update_date
  , CASE 
    WHEN rating < 1 OR rating > 5 THEN NULL 
    ELSE rating
  END AS rating
  , CASE 
    WHEN seo_count < 1 THEN NULL 
    ELSE CAST(seo_count AS INTEGER) 
  END AS seo_count 
FROM 
  `{{ var.value.env_project }}.{{ params['static_features_staging_dataset'] }}.{{ params['dataforseo_table'] }}`
;