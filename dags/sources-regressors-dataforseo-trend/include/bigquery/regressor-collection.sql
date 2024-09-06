CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['web_search_trend_table'] }}`
AS (
  WITH 
    formatting_table AS (
      SELECT 
        CONCAT(fk_sgbrands, '-', type) AS identifier
        , date(local_date) AS local_date
        , value AS factor
      FROM 
        `{{ var.value.env_project }}.{{ params['regressors_staging_dataset'] }}.{{ params['web_search_trend_table'] }}`
    )
    -- Compute the max date and average factor for each identifier
    , Stats AS (
      SELECT
        identifier
        , MAX(local_date) AS max_date
        , AVG(factor) AS avg_factor
      FROM formatting_table
      GROUP BY identifier
    )
    -- Generate future dates from max date
    , FutureDates AS (
      SELECT
        identifier
        , DATE_ADD(max_date, INTERVAL x DAY) AS future_date
      FROM Stats
      JOIN UNNEST(GENERATE_ARRAY(1, 10*30)) AS x   -- Assuming 30 days in a month for simplicity
    )
    -- Compute the future factor using an exponential transition formula
    , FutureFactors AS (
      SELECT
        fd.identifier
        , fd.future_date
        , IF(
          fd.future_date <= DATE_ADD(s.max_date, INTERVAL 7 MONTH)
          , s.avg_factor - (s.avg_factor - o.factor) * EXP(-DATE_DIFF(fd.future_date, s.max_date, DAY)/30.0)  -- Adjust this formula as per your need
          , s.avg_factor
        ) AS future_factor
        FROM FutureDates fd
        JOIN Stats s
          ON fd.identifier = s.identifier
        JOIN formatting_table o
          ON fd.identifier = o.identifier AND s.max_date = o.local_date
    )
    , adding_history_forecast as (
      SELECT 
        identifier
        , local_date
        , factor
      FROM formatting_table
      UNION ALL
      SELECT
        identifier
        , future_date AS local_date
        , future_factor AS factor
      FROM FutureFactors
      ORDER BY 
        identifier
        , local_date
    )
  
  SELECT 
    CONCAT('all', '-', REGEXP_EXTRACT(identifier, r'-([^-]+)$')) AS identifier
    , local_date
    , AVG(factor) as factor
  FROM adding_history_forecast
  GROUP BY 1, 2
  UNION ALL
  SELECT *
  FROM adding_history_forecast
);
