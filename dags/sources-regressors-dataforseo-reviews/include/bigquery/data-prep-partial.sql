CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['static_features_staging_dataset'] }}.{{ params['sgplaceraw_table'] }}`
AS (
  WITH existing_pids AS (
    SELECT pid
    FROM `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    WHERE fk_sgbrands IS NOT NULL
  )
  
  SELECT
    pid
    , CONCAT(name, ', ', street_address, ', ', city, ', ', region) AS keyword
    , ROW_NUMBER() OVER () AS row_num
  FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  WHERE 
    fk_sgbrands IS NOT NULL
    AND pid NOT IN (
        SELECT pid
        FROM existing_pids
    )
)