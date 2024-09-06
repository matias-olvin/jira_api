DECLARE zero_geohits DECIMAL;
DECLARE ratio_raw_and_geohits DECIMAL;
DECLARE ratio_geohits_vs_geohits_last_7_days DECIMAL;

SET zero_geohits = (
  WITH 
    latency_geohits_table AS (
      SELECT *
      FROM
        -- `storage-prod-olvin-com.metrics.latency_geohits`
        `{{ params['project'] }}.{{ params['metrics_dataset'] }}.latency_geohits`
    ),
    latency_raw_tamoco_table AS (
      SELECT *
      FROM
        -- `storage-prod-olvin-com.metrics.latency_raw_tamoco`
        `{{ params['project'] }}.{{ params['metrics_dataset'] }}.latency_raw_tamoco`
    )
  SELECT SUM(row_count)
  FROM latency_geohits_table
  WHERE provider_date = "{{ ds }}"
);
SET ratio_raw_and_geohits = (
  WITH 
    latency_geohits_table AS (
      SELECT *
      FROM
        -- `storage-prod-olvin-com.metrics.latency_geohits`
        `{{ params['project'] }}.{{ params['metrics_dataset'] }}.latency_geohits`
    ),
    latency_raw_tamoco_table AS (
      SELECT *
      FROM
        -- `storage-prod-olvin-com.metrics.latency_raw_tamoco`
        `{{ params['project'] }}.{{ params['metrics_dataset'] }}.latency_raw_tamoco`
    )
  SELECT
    CAST(nb_raw_tamoco / nb_geohits AS DECIMAL)
  FROM (
    SELECT
      SUM(row_count) AS nb_geohits,
      provider_date
    FROM latency_geohits_table
    WHERE provider_date = "{{ ds }}"
    GROUP BY provider_date
  )
  INNER JOIN (
    SELECT
      SUM(volume) AS nb_raw_tamoco,
      provider_date
    FROM latency_raw_tamoco_table
    WHERE provider_date = "{{ ds }}"
    GROUP BY provider_date
  ) 
  USING (provider_date)
);
SET ratio_geohits_vs_geohits_last_7_days = (
  WITH 
    latency_geohits_table AS (
      SELECT *
      FROM
        -- `storage-prod-olvin-com.metrics.latency_geohits`
        `{{ params['project'] }}.{{ params['metrics_dataset'] }}.latency_geohits`
    ),
    latency_raw_tamoco_table AS (
      SELECT *
      FROM
        -- `storage-prod-olvin-com.metrics.latency_raw_tamoco`
        `{{ params['project'] }}.{{ params['metrics_dataset'] }}.latency_raw_tamoco`
    )
  SELECT
    CAST(nb_geohits / nb_geohits_avg_last_7_days AS DECIMAL)
  FROM (
    SELECT
      SUM(row_count) AS nb_geohits,
      provider_date
    FROM latency_geohits_table
    WHERE provider_date = "{{ ds }}"
    GROUP BY provider_date
  ), (
    SELECT
      SUM(row_count) / 7 AS nb_geohits_avg_last_7_days,
    FROM latency_geohits_table
        WHERE provider_date < "{{ ds }}" AND provider_date >= date_sub("{{ ds }}", INTERVAL 7 DAY) 
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
  CAST("{{ ds }}" AS DATE) AS run_date,
  'visits_pipeline_all_daily' AS pipeline,
  'raw to geohits' AS test_name,
  CASE
    WHEN (
      zero_geohits <= 0 OR
      ratio_raw_and_geohits < 0.9999 OR
      (ratio_geohits_vs_geohits_last_7_days > 8 OR
       ratio_geohits_vs_geohits_last_7_days < 0.125)
    ) THEN FALSE
    ELSE TRUE
  END AS result
;

SELECT 
  IF(
    FALSE = (
      zero_geohits <= 0 OR
      ratio_raw_and_geohits < 0.9999 OR
      (ratio_geohits_vs_geohits_last_7_days > 8 OR
      ratio_geohits_vs_geohits_last_7_days < 0.125)
    ),
    TRUE,
    ERROR(FORMAT("raw to geohits error"))
  )
;