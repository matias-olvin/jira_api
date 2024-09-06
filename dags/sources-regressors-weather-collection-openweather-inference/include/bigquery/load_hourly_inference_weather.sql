LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['inference_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
FROM
    FILES (
        FORMAT = 'PARQUET',
        uris = [
            'gs://{{ params["weather_bucket"] }}/input_data/inference_folder/*.parquet'
        ]
    );