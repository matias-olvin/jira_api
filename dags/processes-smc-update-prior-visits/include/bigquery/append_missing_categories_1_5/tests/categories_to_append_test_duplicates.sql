ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                COUNT(*) AS sub_cat_count
            FROM
                `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['categories_to_append_table'] }}`
            GROUP BY
                sub_category
        )
    WHERE
        sub_cat_count > 1
) = 0 AS "Duplicate sub categories found in {{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['categories_to_append_table'] }}"