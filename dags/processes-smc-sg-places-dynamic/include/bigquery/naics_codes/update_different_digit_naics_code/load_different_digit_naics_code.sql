LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['different_digit_naics_code_table'] }}_staging` (
    two_digit_code INTEGER,
    two_digit_title STRING,
    four_digit_code INTEGER,
    four_digit_title STRING,
    six_digit_code INTEGER,
    sub_category STRING
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="uris_xcom_push_different_digit_naics_code_table")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );