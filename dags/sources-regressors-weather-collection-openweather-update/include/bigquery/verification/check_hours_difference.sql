ASSERT (SELECT countif(diff < 3600*95)
FROM (
  SELECT fk_weather_grid_cells, max(hour_ts) - min(hour_ts) as diff
  FROM
  `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
  GROUP BY fk_weather_grid_cells
)) = 0 AS "found difference in hours in {{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}"