LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['new_brands_table'] }}` (
    name STRING,
    pid STRING,
    sub_category STRING,
    median_category_visits INTEGER,
    category_median_visits_range FLOAT64,
    median_brand_visits INTEGER
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="uris_xcom_push_new_brands")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );