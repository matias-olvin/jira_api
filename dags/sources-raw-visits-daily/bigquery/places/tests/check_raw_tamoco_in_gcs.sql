INSERT INTO `{{ params['project'] }}.{{ params['test_compilation_dataset'] }}.{{ params['test_results_table'] }}`
(
  run_date,
  pipeline,
  test_name,
  result
)
SELECT
  DATE("{{ ds }}") AS run_date,
  'visits_pipeline_all_daily' AS pipeline,
  'tamoco raw data in gcs' AS test_name,
  check_tamoco_in_gcs AS result
;