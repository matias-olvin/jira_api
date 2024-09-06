DELETE FROM `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
WHERE
    hour_ts IS NULL;