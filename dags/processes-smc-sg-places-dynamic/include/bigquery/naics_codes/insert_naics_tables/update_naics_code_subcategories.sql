DELETE `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
WHERE
    naics_code IN (
        SELECT
            naics_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['naics_code_subcategories_table'] }}_staging`
    );

INSERT INTO
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
SELECT	
    naics_code,
    sub_category,
    olvin_category,
    essential_retail,
    exclude_bool,
    almanac_category
FROM
    `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['naics_code_subcategories_table'] }}_staging`;