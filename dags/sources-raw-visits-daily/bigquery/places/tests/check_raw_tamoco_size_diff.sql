DECLARE today_size DEFAULT (
  SELECT SUM(volume) 
  FROM `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['tamoco_latency_table'] }}`
  WHERE provider_date = DATE_SUB('{{ ds }}', INTERVAL {{ var.value.latency_days_visits }} DAY)
  GROUP BY provider_date
);
DECLARE yesterday_size DEFAULT (
  SELECT SUM(volume) 
  FROM `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['tamoco_latency_table'] }}`
  WHERE provider_date = DATE_SUB(DATE_SUB('{{ ds }}', INTERVAL {{ var.value.latency_days_visits }} DAY), INTERVAL 1 DAY)
  GROUP BY provider_date
);
DECLARE pct_diff DEFAULT (
  SELECT (
    (today_size-yesterday_size)/
    ((today_size+yesterday_size)/2)
    ) * 100
);
DECLARE check_tamoco_size_diff DEFAULT( 
  SELECT (
    CASE
      WHEN (
        pct_diff < 300 AND 
        pct_diff > -80
      ) THEN TRUE
      ELSE FALSE
    END
  )
);

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
  'tamoco raw data size difference' AS test_name,
  check_tamoco_size_diff AS result
;