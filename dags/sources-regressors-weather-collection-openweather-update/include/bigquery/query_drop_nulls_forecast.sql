DELETE FROM `{{ var.value.env_project }}.{{ params['forecast_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
WHERE
    hour_ts IS NULL;