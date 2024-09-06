ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                pid,
                COUNT(*) AS pid_count
            FROM
                `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}`
            GROUP BY
                pid
        )
    WHERE
        pid_count > 1
) = 0 AS "Duplicate pids in {{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}";