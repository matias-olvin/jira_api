CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['group_visits_dictionary_table'] }}` COPY `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['regressors_dataset'] }}_{{ params['group_visits_dictionary_table'] }}`;

CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['group_visits_dictionary_zipcode_table'] }}` COPY `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['regressors_dataset'] }}_{{ params['group_visits_dictionary_zipcode_table'] }}`;