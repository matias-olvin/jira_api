ASSERT (
    SELECT
        COUNT(naics_code)
    FROM
        `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}`
    WHERE
        naics_code IS NULL
) = 0 AS "Error: one or more naics_code has value NULL in {{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}"