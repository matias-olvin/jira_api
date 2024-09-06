LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['naics_code_subcategories_table'] }}_staging` (
    naics_code INTEGER,
    sub_category STRING,
    olvin_category STRING,
    essential_retail BOOLEAN,
    exclude_bool BOOLEAN,
    almanac_category STRING
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="uris_xcom_push_naics_code_subcategories_table")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );