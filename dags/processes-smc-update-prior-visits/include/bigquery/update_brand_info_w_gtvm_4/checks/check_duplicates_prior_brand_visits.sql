ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                fk_sgbrands,
                COUNT(*) AS fk_sgbrands_count
            FROM
                `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}`
            GROUP BY
                fk_sgbrands
        )
    WHERE
        fk_sgbrands_count > 1
) = 0 AS "Duplicate fk_sgbrands in {{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['prior_brand_visits_table'] }}";