LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['categories_match_table'] }}_staging` (
    naics_code STRING,
    titles STRING,
    total_marketable_US_businesses INTEGER,
    status INTEGER,
    olvin_category INTEGER
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="uris_xcom_push_categories_match_table")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );