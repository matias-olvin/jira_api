ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                COUNT(*) AS naics_code_count
            FROM
                `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}`
            GROUP BY
                naics_code
        )
    WHERE
        naics_code_count > 1
) = 0 AS "Error: naics_code duplicates found in {{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}"