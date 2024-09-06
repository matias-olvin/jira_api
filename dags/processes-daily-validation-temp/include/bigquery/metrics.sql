DECLARE date_to_update DATE;

DECLARE date_to_update_minus_one DATE;

DECLARE date_to_update_last_year DATE;

SET date_to_update = DATE("{{ ds }}");

SET date_to_update_minus_one = DATE_SUB(date_to_update, INTERVAL 1 DAY);

SET date_to_update_last_year = (
  SELECT Last_Year_s_Date 
  FROM `{{ var.value.env_project }}.{{ params['accessible-by-sns-dataset'] }}.{{ params['yoy-dates-nrf-calendar-table'] }}`
  WHERE This_Year_s_Date = date_to_update
);

CREATE TABLE IF NOT EXISTS `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['metrics-table'] }}` (
  local_date DATE
  , median_ratio_error_bias FLOAT64
  , median_abs_ratio_error FLOAT64
  , median_dod_ratio_error_bias FLOAT64
  , median_abs_dod_ratio_error FLOAT64
  , median_yoy_ratio_error_bias FLOAT64
  , median_abs_yoy_ratio_error FLOAT64
);

MERGE INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['metrics-table'] }}` AS target
USING(
  WITH get_visits AS ( 
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
  )
  , daily_visits AS (
    SELECT
      DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date
      , CAST(visits AS FLOAT64) passby_visits
      , fk_sgplaces
    FROM (
      SELECT
        local_date
        , fk_sgplaces
        , JSON_EXTRACT_ARRAY(visits) AS visit_array  -- Get an array from the JSON string
      FROM get_visits
    )
    CROSS JOIN 
      UNNEST(visit_array) AS visits  -- Convert array elements to row
      WITH OFFSET AS row_number  -- Get the position in the array AS another column
    ORDER BY
      local_date
      , fk_sgplaces
      , row_number
  )
  , visits_table AS (
    SELECT *
    FROM (
      SELECT
        fk_sgplaces
        , local_date
        , SUM(visits) AS gt_visits
      FROM `{{ var.value.sns_project }}.{{ params['sns-raw-dataset'] }}.{{ params['sns-raw-traffic-formatted-table'] }}`
      WHERE local_date = date_to_update
      GROUP BY 1, 2
    )
    INNER JOIN  `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}.{{ params['list-sns-pois-table'] }}`
      USING (fk_sgplaces)
    INNER JOIN daily_visits
      USING (fk_sgplaces, local_date)
  )
  SELECT DISTINCT
    local_date
    , PERCENTILE_CONT(error_ratio_bias, 0.5) over (partition by local_date) AS median_ratio_error_bias
    , PERCENTILE_CONT(error_ratio_abs, 0.5) over (partition by local_date) AS median_abs_ratio_error
  FROM (
    SELECT DISTINCT
      fk_sgplaces
      , local_date
      , NULLIF(passby_visits,0) / NULLIF(gt_visits,0) AS error_ratio_bias
      , EXP(ABS(LOG(NULLIF(passby_visits,0) / NULLIF(gt_visits,0)))) AS error_ratio_abs
    FROM visits_table
  )
) AS source
ON target.local_date = source.local_date
WHEN MATCHED THEN
  UPDATE SET
    median_ratio_error_bias = source.median_ratio_error_bias
    , median_abs_ratio_error = source.median_abs_ratio_error
WHEN NOT MATCHED THEN
  INSERT (
    local_date
    , median_ratio_error_bias
    , median_abs_ratio_error
  )
  VALUES (
    source.local_date
    , source.median_ratio_error_bias
    , source.median_abs_ratio_error
  )
;

MERGE INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['metrics-table'] }}` AS target
USING(
  WITH get_visits AS (
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
  )
  , daily_visits as (  -- explode the visits
    SELECT
      DATE_ADD(local_date, INTERVAL row_number DAY) as local_date
      , CAST(visits as FLOAT64) passby_visits
      , fk_sgplaces
    FROM (
      SELECT
        local_date
        , fk_sgplaces
        , JSON_EXTRACT_ARRAY(visits) AS visit_array  -- Get an array from the JSON string
      FROM get_visits  
    )
    CROSS JOIN 
      UNNEST(visit_array) AS visits  -- Convert array elements to row
      WITH OFFSET AS row_number  -- Get the position in the array as another column
    ORDER BY
      local_date
      , fk_sgplaces
      , row_number
  )
  , visits_table AS (
    SELECT *
    FROM (
      SELECT
        fk_sgplaces
        , local_date
        , SUM(visits) AS gt_visits
      FROM `{{ var.value.sns_project }}.{{ params['sns-raw-dataset'] }}.{{ params['sns-raw-traffic-formatted-table'] }}`
    WHERE local_date IN (
      date_to_update
      , date_to_update_minus_one
    )
    GROUP BY 1, 2
    )
    INNER JOIN  `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}.{{ params['list-sns-pois-table'] }}`
      USING (fk_sgplaces)
    INNER JOIN daily_visits
      USING (fk_sgplaces, local_date)
  )
  SELECT DISTINCT
    local_date
    , PERCENTILE_CONT(error_ratio_bias, 0.5) OVER (PARTITION BY local_date) AS median_dod_ratio_error_bias
    , PERCENTILE_CONT(error_ratio_abs, 0.5) OVER (PARTITION BY local_date) AS median_abs_dod_ratio_error
  FROM (
    SELECT
      fk_sgplaces
      , local_date
      , passby_error / gt_error AS error_ratio_bias
      , EXP(ABS(LOG(passby_error / gt_error))) AS error_ratio_abs
    FROM (
      SELECT
        fk_sgplaces
        , local_date
        , NULLIF(gt_visits_this_year, 0) / NULLIF(gt_visits_last_day, 0) AS gt_error
        , NULLIF(passby_visits_this_year, 0) / NULLIF(passby_visits_last_day, 0) AS passby_error
      FROM (
        SELECT *
        FROM (
          SELECT
            fk_sgplaces
            , local_date
            ,  gt_visits AS gt_visits_this_year
            , passby_visits AS passby_visits_this_year
          FROM visits_table
          WHERE local_date = date_to_update
        )
        INNER JOIN (
          SELECT
            fk_sgplaces
            , gt_visits AS gt_visits_last_day
            , passby_visits AS passby_visits_last_day
          FROM visits_table
          WHERE local_date = date_to_update_minus_one
        )
        USING (fk_sgplaces)
      )
    )
  )
) AS source
ON target.local_date = source.local_date
WHEN MATCHED THEN
  UPDATE SET
    median_dod_ratio_error_bias = source.median_dod_ratio_error_bias
    , median_abs_dod_ratio_error = source.median_abs_dod_ratio_error
WHEN NOT MATCHED THEN
  INSERT (
    local_date
    , median_dod_ratio_error_bias
    , median_abs_dod_ratio_error
  )
  VALUES (
    source.local_date
    , source.median_dod_ratio_error_bias
    , source.median_abs_dod_ratio_error
  )
;

MERGE INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['metrics-table'] }}` AS target
USING(
  WITH get_visits AS (
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
  )
  , daily_visits AS (  -- explode the visits
    SELECT
      DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date
      , CAST(visits AS FLOAT64) passby_visits
      , fk_sgplaces
    FROM (
      SELECT
        local_date
        , fk_sgplaces
        , JSON_EXTRACT_ARRAY(visits) AS visit_array  -- Get an array FROM the JSON string
      FROM get_visits
    )
    CROSS JOIN 
      UNNEST(visit_array) AS visits  -- Convert array elements to row
      WITH OFFSET AS row_number  -- Get the position in the array AS another column
    ORDER BY
      local_date
      , fk_sgplaces
      , row_number
  )
  , visits_table AS (
    SELECT *
    FROM (
      SELECT
        fk_sgplaces
        , local_date
        , SUM(visits) AS gt_visits
      FROM `{{ var.value.sns_project }}.{{ params['sns-raw-dataset'] }}.{{ params['sns-raw-traffic-formatted-table'] }}`
      WHERE local_date IN (date_to_update,date_to_update_last_year)
      GROUP BY 1, 2
    )
    INNER JOIN  `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}.{{ params['list-sns-pois-table'] }}`
      USING (fk_sgplaces)
    INNER JOIN daily_visits
      USING (fk_sgplaces, local_date)
  )
  
  SELECT DISTINCT
    local_date
    , PERCENTILE_CONT(error_ratio_bias, 0.5) over (partition by local_date) AS median_yoy_ratio_error_bias
    , PERCENTILE_CONT(error_ratio_abs, 0.5) over (partition by local_date) AS median_abs_yoy_ratio_error
  FROM (
    SELECT 
      fk_sgplaces
      , local_date
      , NULLIF(passby_error, 0) / NULLIF(gt_error, 0) AS error_ratio_bias
      , EXP(ABS(LOG(NULLIF(passby_error, 0) / NULLIF(gt_error, 0)))) AS error_ratio_abs
    FROM (
      SELECT
        fk_sgplaces
        , local_date
        , NULLIF(gt_visits_this_year,0) / NULLIF( gt_visits_last_year, 0) AS gt_error
        , NULLIF(passby_visits_this_year,0) / NULLIF( passby_visits_last_year, 0) AS passby_error
      FROM (
        SELECT *
        FROM (
          SELECT
            fk_sgplaces
            , local_date
            , gt_visits AS gt_visits_this_year
            , passby_visits AS passby_visits_this_year
          FROM visits_table
          WHERE local_date = date_to_update
        )
        INNER JOIN (
          SELECT
            fk_sgplaces
            , gt_visits AS gt_visits_last_year
            , passby_visits AS passby_visits_last_year
          FROM visits_table
          WHERE local_date = date_to_update_last_year
        ) USING (fk_sgplaces)
      )
    )
  )
) AS source
ON target.local_date = source.local_date
WHEN MATCHED THEN
  UPDATE SET
    median_yoy_ratio_error_bias = source.median_yoy_ratio_error_bias
    , median_abs_yoy_ratio_error = source.median_abs_yoy_ratio_error
WHEN NOT MATCHED THEN
  INSERT (
    local_date
    , median_yoy_ratio_error_bias
    , median_abs_yoy_ratio_error
  )
  VALUES (
    source.local_date
    , source.median_yoy_ratio_error_bias
    , source.median_abs_yoy_ratio_error
  )
;