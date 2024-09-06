EXPORT DATA OPTIONS(
    uri = '{{ ti.xcom_pull(task_ids="uris_xcom_push_naics_code_subcategories_table")["query_input_uri"] }}',
    FORMAT = 'CSV',
    overwrite = TRUE,
    header = TRUE,
    compression = 'GZIP'
) AS
SELECT DISTINCT
    places.naics_code,
    places.sub_category,
    olvin_category,
    essential_retail,
    exclude_bool,
    almanac_category
FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` places
    LEFT JOIN `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}` sub ON places.naics_code = sub.naics_code
WHERE
    places.naics_code NOT IN (
        SELECT
            naics_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`
    )
    AND places.naics_code NOT IN (
        SELECT
            naics_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sensitive_naics_codes_table'] }}`
    );