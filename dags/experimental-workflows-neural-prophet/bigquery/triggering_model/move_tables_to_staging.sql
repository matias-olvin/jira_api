CREATE
OR REPLACE TABLE `{{ params['env_project']}}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_estimation_model_dev_dataset'] }}_{{ params['olvin_input'] }}` COPY `{{ params['env_project']}}.{{ params['visits_estimation_model_dev_dataset'] }}.{{ params['olvin_input'] }}`;

CREATE
OR REPLACE TABLE `{{ params['env_project']}}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_estimation_model_dev_dataset'] }}_{{ params['olvin_event_input'] }}` COPY `{{ params['env_project']}}.{{ params['visits_estimation_model_dev_dataset'] }}.{{ params['olvin_event_input'] }}`;

CREATE
OR REPLACE TABLE `{{ params['env_project']}}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_estimation_model_dev_dataset'] }}_{{ params['grouping_id'] }}` COPY `{{ params['env_project']}}.{{ params['visits_estimation_model_dev_dataset'] }}.{{ params['grouping_id'] }}`;