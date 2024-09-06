LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ ti.xcom_pull(task_ids='push_dataset_name_to_xcom') }}.{{ params['categories_to_append_table'] }}` (
    fk_sgbrands STRING,
    sub_category STRING,
    median_category_visits INTEGER,
    category_median_visits_range FLOAT64,
    median_brand_visits INTEGER,
    updated_at DATE
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="append_missing_categories_task_group.uris_xcom_push_categories_to_append")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );