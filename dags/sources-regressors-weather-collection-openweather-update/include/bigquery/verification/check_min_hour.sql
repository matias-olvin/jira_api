ASSERT (
  SELECT
    COUNTIF(diff > 3600)
  FROM
    (
      SELECT
        MAX(min_hour) - MIN(min_hour) AS diff
      FROM
        (
          SELECT
            fk_weather_grid_cells,
            MIN(hour_ts) AS min_hour
          FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
          GROUP BY
            fk_weather_grid_cells
        )
    )
) = 0 AS "Minimum hour check failed for {{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}"