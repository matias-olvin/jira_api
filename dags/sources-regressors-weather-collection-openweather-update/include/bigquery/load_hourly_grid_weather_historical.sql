LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
FROM
    FILES (
        FORMAT = 'PARQUET',
        uris = [
            'gs://{{ params["weather_bucket"] }}/output_data/SILVER/hourly/*.parquet'
        ]
    );