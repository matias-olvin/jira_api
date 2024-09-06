CREATE
OR REPLACE TABLE 
`{{ params['env_project']}}.{{ params['visits_estimation_dataset'] }}.
{{ params['model_output'] }}` 
COPY `{{ params['sns_project']}}.{{ params['accessible_by_olvin_dataset']}}.{{ params['visits_estimation_dataset'] }}_{{ params['model_output'] }}`;