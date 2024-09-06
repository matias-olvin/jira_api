DECLARE test_result BOOLEAN;
SET test_result = (
  WITH
    phq_attendance_count AS (
      SELECT
        COUNT(*) AS total
      FROM
        `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['regressors_table'] }}`
      WHERE
        identifier LIKE CONCAT('%_phq_attendance_', event_type)
    ),
    number_of_events_count AS (
      SELECT
        COUNT(*) AS total
      FROM
        `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['regressors_table'] }}`
      WHERE
        identifier LIKE CONCAT('%_avg_local_rank_', event_type)
    ),
    avg_local_rank_count AS (
      SELECT
        COUNT(*) AS total
      FROM
        `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['regressors_table'] }}`
      WHERE
        identifier LIKE CONCAT('%_avg_local_rank_', event_type)
    )

  SELECT
    (
      phq_attendance_count.total = number_of_events_count.total AND
      avg_local_rank_count.total = number_of_events_count.total
    )
  FROM
    phq_attendance_count,
    number_of_events_count,
    avg_local_rank_count
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
    CONCAT(event_type, ' count') AS test_name,
    test_result AS result
;
