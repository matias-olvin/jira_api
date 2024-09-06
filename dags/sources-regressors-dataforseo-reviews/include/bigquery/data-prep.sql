CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['static_features_staging_dataset'] }}.{{ params['sgplaceraw_table'] }}`
AS (
  SELECT DISTINCT
    fk_sgplaces AS pid
    , CONCAT(name, ', ', street_address, ', ', city, ', ', region) AS keyword
    , ROW_NUMBER() OVER () AS row_num
  FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  WHERE fk_sgbrands IS NOT NULL
)