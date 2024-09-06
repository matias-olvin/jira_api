EXPORT DATA
  OPTIONS( uri="gs://{{ params['visits_estimation_bucket'] }}/ground_truth/supervised/yearly-seasonality/run_date={{ ds }}/input/*.parquet",
    format='PARQUET',
    OVERWRITE=TRUE) AS
SELECT
  *
FROM
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['yearly_seasonality_adjustment_input_table'] }}`