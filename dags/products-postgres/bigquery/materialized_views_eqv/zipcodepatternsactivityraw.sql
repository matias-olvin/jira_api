CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['zipcodepatternsactivity_table'] }}`
AS

SELECT zipcode, tenant_type, sample, percentage, active
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['zipcodepatternsactivity_table'] }}`
ORDER BY zipcode, tenant_type