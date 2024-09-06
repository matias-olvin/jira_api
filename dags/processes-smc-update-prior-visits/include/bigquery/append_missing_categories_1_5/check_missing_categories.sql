ASSERT (
    SELECT
        COUNT(*)
    FROM
        `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['missing_sub_categories_table'] }}`
) = 0 AS "{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['missing_sub_categories_table'] }} is not empty"