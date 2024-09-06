DECLARE test_result BOOLEAN;
SET test_result = (
  WITH
    daily_data AS (
      SELECT
        COUNT(*) AS total
      FROM
        `{{ var.value.env_project }}.{{ params['regressors_staging_dataset'] }}.{{ params['preprocessed_daily_data_table'] }}`
    ),
    upserted_data AS (
      SELECT
        COUNT(*) AS total
      FROM
        `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['full_data_table'] }}`
      WHERE
        DATE(updated) = '{{ ds }}'
    )
    SELECT
      (daily_data.total - upserted_data.total) = 0
    FROM
      daily_data,
      upserted_data
);

INSERT INTO

  `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['test_results_table'] }}`

  (
    run_date,
    regressor,
    test_name,
    result
  )

  SELECT
    CAST("{{ ds }}" AS DATE) AS run_date,
    'regressors_collection_predicthq_events' AS regressor,
    'daily data upsert' AS test_name,
    test_result AS result
;
