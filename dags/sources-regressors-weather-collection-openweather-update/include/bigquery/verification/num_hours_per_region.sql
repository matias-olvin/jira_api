ASSERT (
  SELECT
    COUNTIF(num_hours < 96)
  FROM
    (
      SELECT
        fk_weather_grid_cells,
        COUNT(*) AS num_hours
      FROM
        `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
      GROUP BY
        fk_weather_grid_cells
    )
) = 0 AS "Number of hours per region check failed for {{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}";