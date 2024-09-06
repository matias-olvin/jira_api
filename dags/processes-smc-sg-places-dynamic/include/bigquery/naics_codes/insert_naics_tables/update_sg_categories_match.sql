DELETE `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['categories_match_table'] }}`
WHERE
    naics_code IN (
        SELECT
            naics_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['categories_match_table'] }}_staging`
    );

INSERT INTO
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['categories_match_table'] }}`
SELECT
    naics_code,
    titles,
    total_marketable_US_businesses,
    status,
    olvin_category
FROM
    `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['categories_match_table'] }}_staging`;