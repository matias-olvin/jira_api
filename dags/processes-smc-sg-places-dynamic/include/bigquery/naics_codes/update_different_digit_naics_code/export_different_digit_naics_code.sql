EXPORT DATA OPTIONS(
    uri = '{{ ti.xcom_pull(task_ids="uris_xcom_push_different_digit_naics_code_table")["query_input_uri"] }}',
    FORMAT = 'CSV',
    overwrite = TRUE,
    header = TRUE,
    compression = 'GZIP'
) AS
SELECT DISTINCT
    (places.naics_code) AS six_digit_code,
    two_digit_code,
    two_digit_title,
    four_digit_code,
    four_digit_title,
    places.sub_category,
    places.top_category
FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` places
    LEFT JOIN `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['different_digit_naics_code_table'] }}` dig ON places.naics_code = dig.six_digit_code
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
    );