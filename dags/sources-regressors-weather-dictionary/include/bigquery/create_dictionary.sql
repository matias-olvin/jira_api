CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_regressors_dataset'] }}.{{ params['weather_dictionary_table'] }}`
CLUSTER BY fk_sgplaces, identifier_temperature

AS

SELECT
  fk_sgplaces,
  CONCAT("precip_intensity", "_", fk_weather_grid_cells) AS identifier_precip_intensity,
  CONCAT("wind_speed", "_", fk_weather_grid_cells) AS identifier_wind_speed,
  CONCAT("temperature", "_", fk_weather_grid_cells) AS identifier_temperature,
  CONCAT("humidity", "_", fk_weather_grid_cells) AS identifier_humidity,
  CONCAT("precip_intensity_res", "_", fk_weather_grid_cells) AS identifier_precip_intensity_res,
  CONCAT("wind_speed_res", "_", fk_weather_grid_cells) AS identifier_wind_speed_res,
  CONCAT("temperature_res", "_", fk_weather_grid_cells) AS identifier_temperature_res,
  CONCAT("humidity_res", "_", fk_weather_grid_cells) AS identifier_humidity_res
FROM (
  SELECT
    fk_sgplaces,
    fk_weather_grid_cells,
    ROW_NUMBER() OVER (PARTITION BY fk_sgplaces ORDER BY ST_DISTANCE(point, point_cell)) AS rank_n
  FROM (
    SELECT
      pid AS fk_sgplaces,
      long_lat_point AS point
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  )
  JOIN (
    SELECT
      pid AS fk_weather_grid_cells,
      ST_GEOGPOINT(centre_long, centre_lat) AS point_cell
    FROM
      `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['weather_grid_US_table'] }}`
    UNION ALL
      SELECT
        pid AS fk_weather_grid_cells,
        ST_GEOGPOINT(centre_long, centre_lat) AS point_cell
      FROM
        `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['weather_grid_US_extended_table'] }}`
  )
  ON
    ST_DWITHIN(point, point_cell, 150*1e3)
)
WHERE
  rank_n = 1
