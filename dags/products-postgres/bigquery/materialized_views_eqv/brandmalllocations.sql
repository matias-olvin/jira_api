CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['BrandMallLocations_table'] }}`
AS

SELECT
    c.fk_sgbrands,
    c.state,
    c.primary_RUCA,
    c.brands,
    c.avg_within_5mi,
    c.num_malls_within_5mi,
    c.num_malls_same_RUCA
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['BrandMallLocations_table'] }}` c
ORDER BY c.state