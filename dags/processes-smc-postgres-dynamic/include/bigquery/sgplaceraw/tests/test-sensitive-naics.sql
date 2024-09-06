ASSERT (
    SELECT
                COUNT(*)
            FROM
                `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
            WHERE naics_code NOT IN (
                SELECT 
                    naics_code
                FROM
                    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`
            )
) = 0 AS "Error: NAICS codes in {{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }} do not match those in {{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}"
