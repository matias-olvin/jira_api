ASSERT (
  SELECT
    COUNT(*) - 96
  FROM
    (
      SELECT
        hour_ts,
        COUNT(*) AS num_regions
      FROM
        `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
      GROUP BY
        hour_ts
    )
  WHERE
    num_regions IN (
      SELECT
        MAX(num_regions)
      FROM
        (
          SELECT
            hour_ts,
            COUNT(*) AS num_regions
          FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
          GROUP BY
            hour_ts
        )
    )
) = 0 AS "Number of regions per hour failed for {{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}"