DROP MATERIALIZED VIEW IF EXISTS `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_daily_mv'] }}`;

CREATE MATERIALIZED VIEW `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_daily_mv'] }}` 
PARTITION BY local_date OPTIONS (enable_refresh = false) AS
SELECT
  identifier,
  local_date,
  AVG(factor) AS factor
FROM `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
GROUP BY identifier, local_date;