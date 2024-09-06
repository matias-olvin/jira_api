DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

MERGE INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-visits-table'] }}` AS target
USING(
  WITH get_visits AS ( 
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
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
    INNER JOIN  `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['list-sns-pois-table'] }}`
      USING (fk_sgplaces)
    INNER JOIN daily_visits
      USING (fk_sgplaces, local_date)
  )
  SELECT DISTINCT
    local_date
    , "{{ params['stage'] }}" AS stage
    , "rt" AS process
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
ON
  target.local_date = source.local_date
  AND target.stage = source.stage
  AND target.process = source.process
WHEN MATCHED THEN
  UPDATE SET
    median_ratio_error_bias = source.median_ratio_error_bias
    , median_abs_ratio_error = source.median_abs_ratio_error
WHEN NOT MATCHED THEN
  INSERT (
    local_date
    , stage
    , process
    , median_ratio_error_bias
    , median_abs_ratio_error
  )
  VALUES (
    source.local_date
    , source.stage
    , source.process
    , source.median_ratio_error_bias
    , source.median_abs_ratio_error
  )
;

MERGE INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-visits-table'] }}` AS target
USING(
  WITH get_visits AS ( 
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
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
    INNER JOIN  `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['list-sns-pois-table'] }}`
      USING (fk_sgplaces)
    INNER JOIN daily_visits
      USING (fk_sgplaces, local_date)
  )
  SELECT DISTINCT
    local_date
    , "{{ params['stage'] }}" AS stage
    , "batch" AS process
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
ON
  target.local_date = source.local_date
  AND target.stage = source.stage
  AND target.process = source.process
WHEN MATCHED THEN
  UPDATE SET
    median_ratio_error_bias = source.median_ratio_error_bias
    , median_abs_ratio_error = source.median_abs_ratio_error
WHEN NOT MATCHED THEN
  INSERT (
    local_date
    , stage
    , process
    , median_ratio_error_bias
    , median_abs_ratio_error
  )
  VALUES (
    source.local_date
    , source.stage
    , source.process
    , source.median_ratio_error_bias
    , source.median_abs_ratio_error
  )
;

ASSERT (
  (
    SELECT median_abs_ratio_error <= 1.26
    FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-visits-table'] }}`
    WHERE
      stage = "{{ params['stage'] }}"
      AND process = "rt"
      AND local_date = date_to_update
  )
) AS "median_abs_ratio_error real-time value exceeds 1.26"
;

ASSERT (
  WITH rt AS (
    SELECT median_abs_ratio_error
    FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-visits-table'] }}`
    WHERE
      stage = "{{ params['stage'] }}"
      AND process = "rt"
      AND local_date = date_to_update
  )
  , batch AS (
    SELECT median_abs_ratio_error
    FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-visits-table'] }}`
    WHERE
      stage = "{{ params['stage'] }}"
      AND process = "batch"
      AND local_date = date_to_update
  )
  SELECT rt.median_abs_ratio_error <= batch.median_abs_ratio_error + 0.03
  FROM rt, batch
) AS "median_abs_ratio_error real-time value exceeds batch + 0.03"
;