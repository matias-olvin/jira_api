CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_table'] }}` AS (
  WITH latest AS (
    SELECT
      fk_sgplaces
      , update_date
      , AVG(rating) AS rating
      , AVG(seo_count) AS seo_count
    FROM `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_history_table'] }}` a
    INNER JOIN (
      SELECT
        fk_sgplaces
        , MAX(update_date) AS update_date
      FROM `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['dataforseo_history_table'] }}`
      WHERE
        rating IS NOT NULL 
        AND seo_count IS NOT NULL
      GROUP BY fk_sgplaces
    )
    USING(fk_sgplaces, update_date)
    GROUP BY fk_sgplaces, update_date
  )
  , brand_agg AS (
    SELECT
      pid AS fk_sgplaces
      , rating
      , seo_count
    FROM (
      SELECT
        fk_sgbrands
        , AVG(rating) AS rating
        , APPROX_QUANTILES(seo_count, 2)[OFFSET(1)] AS seo_count
      FROM latest
      INNER JOIN `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
      ON pid=fk_sgplaces
      GROUP BY fk_sgbrands
    )
    INNER JOIN `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    USING(fk_sgbrands)
  )
  , global_agg AS (
    SELECT
      AVG(rating) AS rating
      , APPROX_QUANTILES(seo_count, 2)[OFFSET(1)] AS seo_count
    FROM latest
  )

  SELECT
    fk_sgplaces
    , IFNULL(IFNULL(latest.rating, brand_agg.rating), global_agg.rating) AS rating
    , IFNULL(IFNULL(latest.seo_count, brand_agg.seo_count), global_agg.seo_count) AS seo_count
  FROM (
    SELECT pid AS fk_sgplaces
    FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  )
  LEFT JOIN latest
  USING(fk_sgplaces)
  LEFT JOIN brand_agg
  USING(fk_sgplaces)
  LEFT JOIN global_agg
  ON TRUE
);