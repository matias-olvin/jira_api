DELETE FROM `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
WHERE
    hour_ts IN (
        SELECT DISTINCT
            hour_ts
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
    )
    AND identifier IN (
        SELECT DISTINCT
            CONCAT("precip_intensity", "_", fk_weather_grid_cells)
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
        UNION ALL
        SELECT DISTINCT
            CONCAT(
                "precip_intensity_res",
                "_",
                fk_weather_grid_cells
            )
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
        UNION ALL
        SELECT DISTINCT
            CONCAT("wind_speed", "_", fk_weather_grid_cells)
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
        UNION ALL
        SELECT DISTINCT
            CONCAT("wind_speed_res", "_", fk_weather_grid_cells)
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
        UNION ALL
        SELECT DISTINCT
            CONCAT("temperature", "_", fk_weather_grid_cells)
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
        UNION ALL
        SELECT DISTINCT
            CONCAT("temperature_res", "_", fk_weather_grid_cells)
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
        UNION ALL
        SELECT DISTINCT
            CONCAT("humidity", "_", fk_weather_grid_cells)
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
        UNION ALL
        SELECT DISTINCT
            CONCAT("humidity_res", "_", fk_weather_grid_cells)
        FROM
            `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp`
    );

INSERT INTO
    `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
SELECT
    CONCAT(identifier, "_", fk_weather_grid_cells) AS identifier,
    hour_ts,
    local_date,
    local_hour,
    factor
FROM
    `{{ var.value.env_project }}.{{ params['historical_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp` UNPIVOT(
        factor
        FOR identifier IN (
            precip_intensity,
            wind_speed,
            temperature,
            humidity,
            precip_intensity_res,
            wind_speed_res,
            temperature_res,
            humidity_res
        )
    );