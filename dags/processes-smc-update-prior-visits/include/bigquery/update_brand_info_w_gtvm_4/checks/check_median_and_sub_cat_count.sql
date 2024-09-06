ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                sub_category,
                COUNT(DISTINCT (median_category_visits)) AS _count
            FROM
                `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}`
            GROUP BY
                sub_category
        )
    WHERE
        _count > 1
) = 0 AS "multiple median_category_visits found per sub_category in {{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}";