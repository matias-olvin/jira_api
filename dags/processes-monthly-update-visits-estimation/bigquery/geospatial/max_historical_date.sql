CREATE OR REPLACE TABLE
 `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['max_historical_date_table'] }}`

AS

SELECT
  DATE(max(local_date)) AS max_historical_date
FROM
 `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['visits_aggregated_table'] }}`
