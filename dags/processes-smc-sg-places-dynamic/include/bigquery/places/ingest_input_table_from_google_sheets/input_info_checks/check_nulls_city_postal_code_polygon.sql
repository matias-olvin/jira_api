ASSERT (
    SELECT
        COUNT(*)
    FROM
        `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}`
    WHERE
        city IS NULL
        OR postal_code IS NULL
        OR polygon IS NULL
) = 0 AS "Nulls in city, postal_code or polygon found in {{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}";