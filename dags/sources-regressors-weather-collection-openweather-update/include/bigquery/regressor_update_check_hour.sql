ASSERT (
  SELECT
    COUNT(*) AS count_missing_values
  FROM
    `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}` inp
    LEFT JOIN `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}` reg ON inp.hour_ts = reg.hour_ts
  WHERE
    reg.hour_ts IS NULL
) = 0;