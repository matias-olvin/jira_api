ASSERT (
    SELECT
        COUNT(_count)
    FROM
        (
            SELECT
                COUNT(DISTINCT (fk_sgbrands)) AS _count
            FROM
                `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}`
            GROUP BY
                name
            HAVING
                _count > 1
        )
) = 0 AS "multiple unique fk_sgbrands per brand name found in {{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}";