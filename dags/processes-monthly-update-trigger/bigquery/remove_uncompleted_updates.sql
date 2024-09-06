DELETE FROM `{{ var.value.env_project }}.{{ params['test_compilation_dataset'] }}.{{ params['monthly_update_table'] }}`
WHERE
    NOT completed;