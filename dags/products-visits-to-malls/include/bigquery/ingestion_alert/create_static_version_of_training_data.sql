CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}_static` AS
SELECT
    *
FROM
    `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}`