CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['holidays_collection_table'] }}` COPY `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['regressors_dataset'] }}_{{ params['holidays_collection_table'] }}`;

CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['events_dataset'] }}.{{ params['holidays_collection_table'] }}` COPY `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['events_dataset'] }}_{{ params['holidays_collection_table'] }}`;