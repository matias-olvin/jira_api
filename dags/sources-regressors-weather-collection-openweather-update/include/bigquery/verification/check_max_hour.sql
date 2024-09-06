ASSERT (
  SELECT
    COUNTIF(diff > 3600)
  FROM
    (
      SELECT
        MAX(max_hour) - MIN(max_hour) AS diff
      FROM
        (
          SELECT
            fk_weather_grid_cells,
            MAX(hour_ts) AS max_hour
          FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
          GROUP BY
            fk_weather_grid_cells
        )
    )
) = 0 AS "max hour check failed for {{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}";