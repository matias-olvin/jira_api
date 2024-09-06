ASSERT (
    SELECT
        COUNT(pid)
    FROM
        `{{ var.value.env_project }}.{{ params['visits_to_malls_staging_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}`
    WHERE
        pid IS NULL
) = 0 AS "null pids found in {{ var.value.env_project }}.{{ params['visits_to_malls_staging_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}";