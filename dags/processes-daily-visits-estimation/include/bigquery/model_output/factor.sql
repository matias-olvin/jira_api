DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

CREATE OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-output-factor-table'] }}` AS (
  WITH events_adjustment_usage AS (
    SELECT
      fk_sgplaces
      , local_date
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['adjustments-events-usage-table'] }}`
    WHERE local_date = date_to_update
    AND modified_in_events = False
  )
  , batch_output_table AS (
    SELECT
      fk_sgplaces
      , local_date
      , prediction AS batch_prediction
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['batch-supervised-output-table'] }}`
    WHERE local_date = date_to_update
  )
  , real_time_output_table AS (
    SELECT
      fk_sgplaces
      , local_date
      , prediction AS real_time_prediction
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-supervised-output-table'] }}`
    WHERE local_date = date_to_update
  )
  -- see documentation to understand the logic 0.01
  SELECT
    fk_sgplaces
    , local_date
    , (
      IF((real_time_prediction > 0) OR (real_time_prediction IS NULL), real_time_prediction, 0) / 
      IF(batch_prediction > 0.01, batch_prediction, NULL)
    ) AS factor
  FROM (
    SELECT *
    FROM batch_output_table
    INNER JOIN real_time_output_table USING (fk_sgplaces, local_date)
    INNER JOIN events_adjustment_usage USING (fk_sgplaces, local_date)
  )
);

DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-output-factors-distribution-table'] }}`
WHERE
  run_date = DATE("{{ ds }}")
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;
INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-output-factors-distribution-table'] }}`
SELECT DISTINCT
  PERCENTILE_CONT(factor, 0.05) OVER () AS perc_05_factor
  , PERCENTILE_CONT(factor, 0.2) OVER () AS perc_20_factor
  , PERCENTILE_CONT(factor, 0.5) OVER () AS median_factor
  , PERCENTILE_CONT(factor, 0.7) OVER () AS perc_70_factor
  , PERCENTILE_CONT(factor, 0.95) OVER () AS perc_95_factor
  , "{{ params['stage'] }}" AS stage
  , date_to_update AS local_date
  , DATE("{{ ds }}") AS run_date
FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-output-factor-table'] }}`
;