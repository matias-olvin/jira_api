DROP MATERIALIZED VIEW IF EXISTS `{{ var.value.env_project }}.{{ params['validations_almanac_dataset'] }}.{{ params['almanac_volume_mv'] }}`;

CREATE OR REPLACE MATERIALIZED VIEW
  `{{ var.value.env_project }}.{{ params['validations_almanac_dataset'] }}.{{ params['almanac_volume_mv'] }}`
PARTITION BY
  run_date
CLUSTER BY
  id OPTIONS(enable_refresh = FALSE) AS
SELECT
  vol.*,
  sg_place.*
FROM
  `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['volume_metrics_table'] }}` AS vol
  JOIN `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` AS sg_place ON vol.id = sg_place.fk_sgbrands;