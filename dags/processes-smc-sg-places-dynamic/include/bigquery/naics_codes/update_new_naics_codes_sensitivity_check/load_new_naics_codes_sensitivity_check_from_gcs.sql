LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}` (
    naics_code INTEGER,
    top_category STRING,
    sub_category STRING,
    sensitive BOOLEAN
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="uris_xcom_push_new_naics_codes_sensitivity_check_table")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );