ASSERT (
    SELECT
        COUNT(six_digit_code)
    FROM
        `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['different_digit_naics_code_table'] }}_staging`
    WHERE
        six_digit_code IS NULL
) = 0 AS "Error: one or more six_digit_code are NULL in {{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['different_digit_naics_code_table'] }}_staging"