DECLARE current_execution_date DATE DEFAULT (
    SELECT
        DATE_ADD(MAX(execution_date), INTERVAL 1 MONTH)
    FROM
        `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['monthly_update_table'] }}`
    WHERE
        completed
);
DECLARE today DATE DEFAULT CURRENT_DATE();
DECLARE geospatial_latency INT64 DEFAULT 10;
DECLARE ground_truth_latency INT64 DEFAULT 3;
DECLARE first_monday_next_month DATE DEFAULT(
  SELECT
  CASE WHEN DATE_TRUNC(DATE_ADD(DATE_TRUNC(DATE_ADD(current_execution_date, INTERVAL 1 MONTH), WEEK), INTERVAL 1 DAY), MONTH)
                              = DATE_TRUNC(DATE_ADD(current_execution_date, INTERVAL 1 MONTH), MONTH)
  THEN DATE_ADD(DATE_TRUNC(DATE_ADD(current_execution_date, INTERVAL 1 MONTH), WEEK), INTERVAL 1 DAY)
  ELSE DATE_ADD(DATE_TRUNC(DATE_ADD(current_execution_date, INTERVAL 1 MONTH), WEEK), INTERVAL 8 DAY)
  END
);
-- First Monday of next month:
--   - If Sunday before next month's 1st day (current_execution_day + 1 month) + 1 day is already next month that means that the month starts on Monday
--   - Otherwise, add 8 days to that "Sunday before 1st day of next month" to get 1st Monday of next month


INSERT INTO
    `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['monthly_update_table'] }}`
    (execution_date, completed, start_running_date, latest_geospatial_date, latest_ground_truth_date, push_to_prod_date, end_running_date)
VALUES
    (current_execution_date, FALSE, today,
    DATE_SUB(today, INTERVAL geospatial_latency DAY),
    DATE_SUB(today, INTERVAL ground_truth_latency DAY),
    first_monday_next_month,
    DATE_ADD(current_execution_date, INTERVAL 1 MONTH));