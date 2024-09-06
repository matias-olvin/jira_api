ASSERT (
    SELECT
        COUNT(naics_code)
    FROM
        `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['categories_match_table'] }}_staging`
    WHERE
        naics_code IS NULL
) = 0 AS "Error: one or more naics_codes are NULL in {{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['categories_match_table'] }}_staging"