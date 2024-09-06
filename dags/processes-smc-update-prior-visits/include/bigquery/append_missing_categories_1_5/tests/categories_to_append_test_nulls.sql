ASSERT (
    SELECT
        COUNT(*)
    FROM
        `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['categories_to_append_table'] }}`
    WHERE
        median_category_visits IS NULL
        OR category_median_visits_range IS NULL
        OR sub_category IS NULL
) = 0 AS "Null values found in sub_category, median_category_visits or category_median_visits_range in {{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['categories_to_append_table'] }}"