CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['web_search_trend_dictionary_table'] }}`
AS (
  SELECT
    pid AS fk_sgplaces
    , IFNULL(identifier_ws, 'all-ws') AS identifier_ws
    , IFNULL(identifier_gs, 'all-gs') AS identifier_gs
    , IFNULL(identifier_ws_near_me, 'all-ws_near_me') AS identifier_ws_near_me
  FROM 
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  LEFT JOIN (
    SELECT DISTINCT
      fk_sgbrands
      , CONCAT(fk_sgbrands, '-', 'ws' ) AS identifier_ws
    FROM 
        `{{ var.value.env_project }}.{{ params['regressors_staging_dataset'] }}.{{ params['web_search_trend_table'] }}`
    WHERE type = 'ws'
  ) USING (fk_sgbrands)
  LEFT JOIN (
    SELECT DISTINCT
      fk_sgbrands
      , CONCAT(fk_sgbrands, '-', 'gs' ) AS identifier_gs
    FROM 
      `{{ var.value.env_project }}.{{ params['regressors_staging_dataset'] }}.{{ params['web_search_trend_table'] }}`
    WHERE type = 'gs'
  ) USING (fk_sgbrands)
  LEFT JOIN (
    SELECT DISTINCT
      fk_sgbrands
      , CONCAT(fk_sgbrands, '-', 'ws_near_me' ) AS identifier_ws_near_me
    FROM
      `{{ var.value.env_project }}.{{ params['regressors_staging_dataset'] }}.{{ params['web_search_trend_table'] }}`
    WHERE type = 'ws_near_me'
  ) USING (fk_sgbrands)
);
