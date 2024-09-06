DECLARE latency_test_result BOOLEAN;
SET latency_test_result = (
  WITH table1 AS (
    SELECT
      distinct chicago_local_date,
      processed_in_cluster,
      SUM(volume) OVER (PARTITION BY chicago_local_date, processed_in_cluster) / SUM(volume) OVER (PARTITION BY chicago_local_date) AS part_processed
    FROM (
      SELECT 
        *,
        IF(DATE_ADD(chicago_local_date, INTERVAL 10 DAY) > CAST(provider_date AS DATE), TRUE, FALSE) AS processed_in_cluster,
      FROM 
        `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['tamoco_latency_table'] }}`
      WHERE 
        country = 'US' AND
        chicago_local_date = DATE_SUB("{{ ds }}", INTERVAL 20 DAY) 
    )
  )
  SELECT part_processed >= 0.95
  FROM table1
  WHERE processed_in_cluster IS TRUE 
);

INSERT INTO `{{ params['project'] }}.{{ params['test_compilation_dataset'] }}.{{ params['test_results_table'] }}`
(
  run_date,
  pipeline,
  test_name,
  result
)
SELECT
  CAST("{{ ds }}" AS DATE) AS run_date,
  'visits_pipeline_all_daily' AS pipeline,
  '10 day latency' AS test_name,
  latency_test_result AS result
;