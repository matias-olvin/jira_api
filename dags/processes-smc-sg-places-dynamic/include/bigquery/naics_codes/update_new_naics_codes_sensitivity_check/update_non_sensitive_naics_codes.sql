DELETE `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`
WHERE
    naics_code IN (
        SELECT
            naics_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}`
        WHERE
            sensitive = FALSE
    );

INSERT INTO
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`
SELECT
    naics_code
FROM
    `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}`
WHERE
    sensitive = FALSE;