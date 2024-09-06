ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                fk_sgplaces,
                COUNT(*) AS duplicates
            FROM
                `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}`
            GROUP BY
                fk_sgplaces
            HAVING
                duplicates > 1
        )
) = 0 AS "Duplicate fk_sgplaces found in {{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}";