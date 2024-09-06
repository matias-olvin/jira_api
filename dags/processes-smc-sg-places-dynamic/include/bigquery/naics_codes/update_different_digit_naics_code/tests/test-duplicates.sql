ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                COUNT(*) AS _count
            FROM
                `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['different_digit_naics_code_table'] }}_staging`
            GROUP BY
                six_digit_code
        )
    WHERE
        _count > 1
) = 0 AS "Error: six_digit_code duplicates found in {{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['different_digit_naics_code_table'] }}_staging";