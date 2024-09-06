ASSERT (
    SELECT
        COUNT(*)
    FROM
        `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}`
    WHERE
        median_category_visits IS NULL
        OR category_median_visits_range IS NULL
        OR pid IS NULL
) = 0 AS "Nulls found in pid, median_category_visits or category_median_visits_range columns in {{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}";