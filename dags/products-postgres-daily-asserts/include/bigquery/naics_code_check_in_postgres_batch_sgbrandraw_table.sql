ASSERT (
    SELECT
        COUNT(*)
    FROM
        `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}`
    WHERE
        naics_code NOT IN (
            SELECT
                naics_code
            FROM
                `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`
        )
) = 0 AS "potentially sensitive naics code found in {{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}";