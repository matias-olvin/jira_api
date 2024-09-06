DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

BEGIN CREATE TEMP TABLE daily_visits AS (
  WITH get_visits AS (
    SELECT * 
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
  )
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date
    , CAST(visits AS FLOAT64) AS batch_visits
    , fk_sgplaces
  FROM (
    SELECT
      local_date
      , fk_sgplaces
      , JSON_EXTRACT_ARRAY(visits) AS visit_array  -- Get an array from the JSON string
    FROM get_visits    
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert array elements to row
    WITH OFFSET AS row_number -- Get the position in the array AS another column
  ORDER BY
    local_date
    , fk_sgplaces
    , row_number
); END;

BEGIN CREATE TEMP TABLE clamping_to_avoid_unexpected_behaviour AS (
  WITH adding_scaled_visits AS (
    SELECT
      *
      , IFNULL(batch_visits / NULLIF(AVG(batch_visits) OVER (PARTITION BY fk_sgplaces), 0), 0) AS batch_visits_scaled
      , AVG(batch_visits) OVER (PARTITION BY fk_sgplaces) AS scaling_factor
    FROM daily_visits
  )
  , daily_visits_to_update AS (
    SELECT *
    FROM adding_scaled_visits
    WHERE local_date = date_to_update
  )
  , applying_factor AS (
    SELECT
      fk_sgplaces
      , local_date
      , batch_visits * IFNULL(factor, 1) AS visits
      , batch_visits
      , batch_visits_scaled
      , scaling_factor
    FROM daily_visits_to_update
    LEFT JOIN `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-output-factor-table'] }}`
      USING (fk_sgplaces, local_date)
  )
  SELECT
    fk_sgplaces
    , local_date
    , IF(
      IFNULL(visits / NULLIF(scaling_factor, 0), 0) < PERCENTILE_CONT(batch_visits_scaled, 0.999) OVER ()
      , visits
      , GREATEST(batch_visits, scaling_factor * PERCENTILE_CONT(batch_visits_scaled, 0.999) OVER ())
    ) AS visits
    , batch_visits
  FROM applying_factor
); END;

DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-output-pois-unchanged-table'] }}`
WHERE
  run_date = DATE("{{ ds }}")
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;
INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-output-pois-unchanged-table'] }}`
SELECT
  fk_sgplaces
  , "{{ params['stage'] }}" AS stage
  , date_to_update AS local_date
  , DATE("{{ ds }}") AS run_date
FROM clamping_to_avoid_unexpected_behaviour
WHERE visits = batch_visits
;

CREATE OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-algo-output-table'] }}` AS (
  WITH final_daily_table AS (
    SELECT
      fk_sgplaces
      , local_date
      , CAST(visits AS INT64) AS visits
    FROM clamping_to_avoid_unexpected_behaviour
  )
  , test_table AS (
    SELECT *
    FROM
      final_daily_table
      , (SELECT COUNT(*) AS input_count FROM daily_visits where local_date = date_to_update)
      , (SELECT COUNT(*) AS output_count FROM final_daily_table)
      , (SELECT COUNT(*) AS input_count_distinct  FROM (SELECT distinct fk_sgplaces, local_date FROM daily_visits where local_date = date_to_update  ))
      , (SELECT COUNT(*) AS output_count_distinct  FROM ( SELECT distinct fk_sgplaces, local_date FROM final_daily_table))
      , (SELECT COUNT(*) AS output_count_nulls FROM final_daily_table where visits IS NULL)
  )
  SELECT * EXCEPT(
    input_count
    , output_count
    , output_count_distinct
    , input_count_distinct
    , output_count_nulls
  )
  FROM test_table
  WHERE IF(
    (input_count = output_count) AND (input_count_distinct = output_count_distinct) AND (output_count_nulls = 0)
    , TRUE
    , ERROR(FORMAT(
      "no same rows in batch than in real time  %d <> %d , or no same distinct fk_sgplaces in batch than in real time  %d <> %d  or some values are null %d "
      , output_count
      , input_count
      , output_count_distinct
      , input_count_distinct
      , output_count_nulls
    ))
  )
);
