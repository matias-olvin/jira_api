EXPORT DATA OPTIONS(
    uri = '{{ ti.xcom_pull(task_ids="uris_xcom_push_new_naics_codes_sensitivity_check_table")["query_input_uri"] }}',
    FORMAT = 'CSV',
    overwrite = TRUE,
    header = TRUE,
    compression = 'GZIP'
) AS
SELECT DISTINCT
    naics_code,
    top_category,
    sub_category,
    TRUE AS sensitive
FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
WHERE
    naics_code NOT IN (
        SELECT
            naics_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`
    )
    AND naics_code NOT IN (
        SELECT
            naics_code
        FROM
            `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sensitive_naics_codes_table'] }}`
    )