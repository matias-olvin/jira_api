DROP MATERIALIZED VIEW IF EXISTS `{{ var.value.env_project }}.{{ params['validations_almanac_dataset'] }}.{{ params['almanac_trend_mv'] }}`;

CREATE OR REPLACE MATERIALIZED VIEW
  `{{ var.value.env_project }}.{{ params['validations_almanac_dataset'] }}.{{ params['almanac_trend_mv'] }}`
PARTITION BY
  run_date
CLUSTER BY
  id OPTIONS(enable_refresh = FALSE) AS
SELECT
  trend.*,
  sg_place.*,
  CASE
    WHEN correlation[SAFE_OFFSET(0)].days > 1090
    AND correlation[SAFE_OFFSET(1)].days IS NULL THEN "All Time"
    WHEN correlation[SAFE_OFFSET(0)].days > 1090
    AND correlation[SAFE_OFFSET(1)].days = 365 THEN "1 Year"
    WHEN correlation[SAFE_OFFSET(0)].days > 720 THEN "2 Year"
    WHEN correlation[SAFE_OFFSET(0)].days > 99 THEN "6 Month"
    ELSE "3 Month"
  END AS duration
FROM
  `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}` AS trend
  JOIN `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` AS sg_place ON trend.id = sg_place.pid;