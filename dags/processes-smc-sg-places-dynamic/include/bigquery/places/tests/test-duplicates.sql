ASSERT (SELECT COUNT(*)
    FROM (
        SELECT
            COUNT(*) AS pid_count
        FROM
            `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
        GROUP BY
            pid
    )
    WHERE
        pid_count > 1
) = 0 AS "Error: pid duplicates found in {{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}"
