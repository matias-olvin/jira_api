CREATE OR REPLACE TABLE `{{ params['env_project'] }}.{{ params['visits_estimation_model_dev_dataset'] }}.{{ params['olvin_input'] }}`
partition by local_date
CLUSTER BY fk_sgplaces
AS
SELECT * FROM `{{ params['env_project'] }}.{{ params['visits_estimation_model_dev_dataset'] }}.{{ params['olvin_input'] }}_20*`;