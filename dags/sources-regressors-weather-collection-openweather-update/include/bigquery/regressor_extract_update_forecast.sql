CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['forecast_weather_dataset'] }}.{{ params['hourly_grid_table'] }}_temp` AS
WITH
  raw_table AS (
    SELECT
      hour_ts,
      local_date,
      local_hour,
      fk_weather_grid_cells,
      precip_intensity - precip_intensity_res AS precip_intensity_28,
      wind_speed - wind_speed_res AS wind_speed_28,
      temperature - temperature_res AS temperature_hat,
      humidity - humidity_res AS humidity_28,
    FROM
      (
        SELECT
          * EXCEPT (identifier, fk_weather_grid_cells),
          REGEXP_REPLACE(
            identifier,
            CONCAT("_", fk_weather_grid_cells),
            ""
          ) AS identifier,
          CAST(fk_weather_grid_cells AS INT) AS fk_weather_grid_cells,
        FROM
          (
            SELECT
              *,
              ARRAY_REVERSE(SPLIT(identifier, "_")) [SAFE_OFFSET(0)] AS fk_weather_grid_cells
            FROM
              `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
            WHERE
              local_date >= "{{ ds }}"
              AND local_date < "{{ next_execution_date.add(days=8).strftime('%Y-%m-%d') }}"
          )
      ) PIVOT(
        AVG(factor)
        FOR identifier IN (
          "precip_intensity",
          "precip_intensity_res",
          "wind_speed",
          "wind_speed_res",
          "temperature",
          "temperature_res",
          "humidity",
          "humidity_res"
        )
      )
  ),
  avg_table AS (
    SELECT
      fk_weather_grid_cells,
      hour_ts,
      fk_met_areas,
      local_date,
      local_hour,
      AVG(
        IF(IS_NAN(precipIntensity), NULL, precipIntensity)
      ) AS precipIntensity,
      AVG(IF(IS_NAN(windSpeed), NULL, windSpeed)) AS windSpeed,
      AVG(IF(IS_NAN(temperature), NULL, temperature)) AS temperature,
      AVG(IF(IS_NAN(humidity), NULL, humidity)) AS humidity,
    FROM
      (
        SELECT
          * EXCEPT (hour_ts),
          IF(
            IS_NAN(hour_ts)
            OR hour_ts IS NULL,
            UNIX_SECONDS(
              TIMESTAMP(
                FORMAT_TIMESTAMP(
                  "%F %T",
                  TIMESTAMP_ADD(TIMESTAMP(local_date), INTERVAL local_hour HOUR)
                ),
                TIMEZONE
              )
            ),
            hour_ts
          ) AS hour_ts,
        FROM
          `{{ var.value.env_project }}.{{ params['forecast_weather_dataset'] }}.{{ params['hourly_grid_table'] }}`
          JOIN (
            SELECT
              pid AS fk_weather_grid_cells,
              TIMEZONE
            FROM
              `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['weather_grid_US_table'] }}`
          ) USING (fk_weather_grid_cells)
      )
    GROUP BY
      fk_weather_grid_cells,
      hour_ts,
      fk_met_areas,
      local_date,
      local_hour
  ),
  reference_cells AS (
    SELECT DISTINCT
      fk_weather_grid_cells,
      fk_met_areas
    FROM
      avg_table
  ),
  reference_times AS (
    SELECT DISTINCT
      local_date,
      local_hour
    FROM
      avg_table
  ),
  new_data_table AS (
    SELECT
      fk_weather_grid_cells,
      hour_ts,
      local_date,
      local_hour,
      IFNULL(
        IFNULL(
          IFNULL(
            precipIntensity,
            AVG(precipIntensity) OVER hour_area
          ),
          AVG(precipIntensity) OVER day_area
        ),
        0
      ) AS precip_intensity,
      IFNULL(
        IFNULL(
          IFNULL(windSpeed, AVG(windSpeed) OVER hour_area),
          AVG(windSpeed) OVER day_area
        ),
        0
      ) AS wind_speed,
      IFNULL(
        IFNULL(
          IFNULL(temperature, AVG(temperature) OVER hour_area),
          AVG(temperature) OVER day_area
        ),
        52.7
      ) AS temperature,
      IFNULL(
        IFNULL(
          IFNULL(humidity, AVG(humidity) OVER hour_area),
          AVG(humidity) OVER day_area
        ),
        0
      ) AS humidity,
    FROM
      avg_table
      FULL OUTER JOIN (
        SELECT
          *
        FROM
          reference_cells
          CROSS JOIN reference_times
      ) USING (
        fk_weather_grid_cells,
        fk_met_areas,
        local_date,
        local_hour
      )
    WINDOW
      hour_area AS (
        PARTITION BY
          hour_ts,
          fk_met_areas,
          local_date,
          local_hour
      ),
      day_area AS (
        PARTITION BY
          fk_met_areas,
          local_date
      )
  )
SELECT
  fk_weather_grid_cells,
  local_hour,
  local_date,
  hour_ts,
  precip_intensity,
  IF(
    precip_intensity_28 IS NULL,
    0,
    precip_intensity - precip_intensity_28
  ) AS precip_intensity_res,
  wind_speed,
  IF(
    wind_speed_28 IS NULL,
    0,
    wind_speed - wind_speed_28
  ) AS wind_speed_res,
  temperature,
  IF(
    temperature_hat IS NULL,
    0,
    temperature - temperature_hat
  ) AS temperature_res,
  humidity,
  IF(humidity_28 IS NULL, 0, humidity - humidity_28) AS humidity_res
FROM
  raw_table
  RIGHT JOIN new_data_table USING (
    fk_weather_grid_cells,
    local_hour,
    local_date,
    hour_ts
  )