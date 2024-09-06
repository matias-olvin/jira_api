LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['visits_to_malls_staging_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}` (
    pid STRING,
    name STRING,
    city STRING,
    region STRING,
    GLA INTEGER,
    TOTSTORES INTEGER,
    LEVELS INTEGER,
    visitors_annually_M INTEGER,
    adjusted_visitors_annually_M FLOAT64
)
FROM
    FILES (
        FORMAT = 'CSV',
        compression = 'GZIP',
        uris = [
            '{{ ti.xcom_pull(task_ids="ingest_training_data_from_google_sheets.push_uris_to_xcom")["query_output_uri"] }}'
        ],
        skip_leading_rows = 1
    );