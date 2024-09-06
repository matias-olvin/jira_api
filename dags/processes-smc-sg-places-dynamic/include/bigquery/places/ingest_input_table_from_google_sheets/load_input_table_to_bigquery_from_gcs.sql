LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}` (
    added_date DATE,
    fk_sgplaces STRING,
    fk_sgbrands STRING,
    opening_date DATE,
    fk_sgcenters STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    polygon STRING,
    open_hours STRING,
    closing_date DATE,
    name STRING,
    street_address STRING,
    enclosed BOOLEAN,
    includes_parking_lot BOOLEAN,
    phone_number INTEGER,
    industry STRING,
    category_tags STRING,
    city STRING,
    region STRING,
    postal_code	STRING
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="ingest_input_table_from_google_sheets_task_group.push_uris_to_xcom")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );