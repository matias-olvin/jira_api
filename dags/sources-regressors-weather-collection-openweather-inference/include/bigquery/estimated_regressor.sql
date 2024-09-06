CREATE OR REPLACE TABLE
`{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
PARTITION BY local_date CLUSTER BY identifier, hour_ts AS
WITH raw_table AS (
  SELECT
    *
  FROM
    (
      SELECT
        *
      EXCEPT(identifier, fk_weather_grid_cells),
        REGEXP_REPLACE(
          identifier,
          CONCAT("_", fk_weather_grid_cells),
          ""
        ) AS identifier,
        CAST(fk_weather_grid_cells AS INT) AS fk_weather_grid_cells,
        MOD(
          EXTRACT(
            DAYOFYEAR
            FROM
              local_date
          ) + 365 -1,
          365
        ) + 1 AS day_of_year,
        -- Deal with leap years
      FROM
        (
          SELECT
            *,
            ARRAY_REVERSE(SPLIT(identifier, "_")) [SAFE_OFFSET(0)] AS fk_weather_grid_cells
          FROM
            `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
        )
    ) PIVOT(
      AVG(factor) FOR identifier IN (
        "precip_intensity",
        "wind_speed",
        "temperature",
        "humidity"
      )
    )
),
year_table AS (
  SELECT
    EXTRACT(
      DAYOFYEAR
      FROM
        local_date
    ) AS day_of_year_org,
    local_hour,
    fk_weather_grid_cells,
    AVG(precip_intensity) AS precip_intensity,
    AVG(wind_speed) AS wind_speed,
    AVG(temperature) AS temperature,
    AVG(humidity) AS humidity,
  FROM
    raw_table
  WHERE
    local_date <= CURRENT_DATE()
  GROUP BY
    fk_weather_grid_cells,
    local_date,
    local_hour
),
averages_table AS (
  SELECT
    fk_weather_grid_cells,
    day_of_year,
    local_hour,
    SUM(precip_intensity) / SUM(precip_intensity_factor) AS precip_intensity_28,
    SUM(wind_speed) / SUM(wind_speed_factor) AS wind_speed_28,
    SUM(temperature) / SUM(temperature_factor) AS temperature_28,
    SUM(humidity) / SUM(humidity_factor) AS humidity_28,
  FROM
    (
      SELECT
        *
      FROM
        (
          SELECT
            local_hour,
            MOD(day_of_year_org + days_difference + 365 -1, 365) + 1 AS day_of_year,
            fk_weather_grid_cells,
            precip_intensity * (
              0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)
            ) AS precip_intensity,
            IF(precip_intensity IS NULL, NULL, 1) *(0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)) AS precip_intensity_factor,
            wind_speed * (
              0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)
            ) AS wind_speed,
            IF(wind_speed IS NULL, NULL, 1) *(0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)) AS wind_speed_factor,
            temperature * (
              0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)
            ) AS temperature,
            IF(temperature IS NULL, NULL, 1) *(0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)) AS temperature_factor,
            humidity * (
              0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)
            ) AS humidity,
            IF(humidity IS NULL, NULL, 1) *(0.5 + 0.5 * COS(days_difference * ACOS(-1) / 28)) AS humidity_factor,
          FROM
            year_table
            CROSS JOIN UNNEST(
              GENERATE_ARRAY(- 28, 28, 1)
            ) AS days_difference
        )
    )
  GROUP BY
    fk_weather_grid_cells,
    day_of_year,
    local_hour
),
historical_table AS (
  SELECT
    fk_weather_grid_cells,
    local_date,
    local_hour,
    IF(
      IS_NAN(hour_ts) OR hour_ts IS NULL,
      UNIX_SECONDS(TIMESTAMP(FORMAT_TIMESTAMP("%F %T", TIMESTAMP_ADD(TIMESTAMP(local_date), INTERVAL local_hour HOUR )), timezone)),
      hour_ts
    ) AS hour_ts,
    precip_intensity,
    IF(precip_intensity_28 IS NULL, 0, precip_intensity - precip_intensity_28) AS precip_intensity_res,
    wind_speed,
    IF(wind_speed_28 IS NULL, 0, wind_speed - wind_speed_28) AS wind_speed_res,
    temperature,
    IF(temperature_hat IS NULL, 0, temperature - temperature_hat) AS temperature_res,
    humidity,
    IF(humidity_28 IS NULL, 0, humidity - humidity_28) AS humidity_res
  FROM
    raw_table
    LEFT JOIN averages_table USING (fk_weather_grid_cells, local_hour, day_of_year)
    LEFT JOIN
      `{{ var.value.env_project }}.{{ params['inference_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
    USING(
      fk_weather_grid_cells,
      local_date,
      local_hour,
      hour_ts
    )
    JOIN (
          SELECT
            pid AS fk_weather_grid_cells,
            timezone
          FROM
            `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['weather_grid_US_table'] }}`
          UNION ALL
          SELECT
            pid AS fk_weather_grid_cells,
            timezone
          FROM
            `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['weather_grid_US_table'] }}_extended`
        ) USING(fk_weather_grid_cells)
  WHERE
    local_date <= CURRENT_DATE()
),
forecast_table AS (
  SELECT
    fk_weather_grid_cells,
    local_date,
    local_hour,
    hour_ts,
    precip_intensity_28 AS precip_intensity,
    0 AS precip_intensity_res,
    wind_speed_28 AS wind_speed,
    0 AS wind_speed_res,
    temperature_hat AS temperature,
    0 AS temperature_res,
    humidity_28 AS humidity,
    0 AS humidity_res,
  FROM
    (
      SELECT
        *,
        MOD(
          EXTRACT(
            DAYOFYEAR
            FROM
              local_date
          ) + 365 -1,
          365
        ) + 1 AS day_of_year,
      FROM
        `{{ var.value.env_project }}.{{ params['inference_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
    )
    JOIN averages_table USING (fk_weather_grid_cells, local_hour, day_of_year)
  WHERE
    local_date > CURRENT_DATE()
),
total_table AS (
  SELECT
    *
  FROM
    historical_table
  UNION ALL
  SELECT
    *
  FROM
    forecast_table
)
SELECT
  CONCAT(identifier, "_", fk_weather_grid_cells) AS identifier,
  hour_ts,
  local_date,
  local_hour,
  factor
FROM
  total_table UNPIVOT(
    factor FOR identifier IN (
      precip_intensity,
      precip_intensity_res,
      wind_speed,
      wind_speed_res,
      temperature,
      temperature_res,
      humidity,
      humidity_res
    )
  )
