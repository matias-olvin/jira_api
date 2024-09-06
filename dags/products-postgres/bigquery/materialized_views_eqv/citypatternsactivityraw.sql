CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['postgres_mv_dataset'] }}.{{ params['citypatternsactivity_table'] }}`
  --`storage-prod-olvin-com.postgres_materialize_views.CityPatternsActivityRaw`
AS

SELECT city, state_abbreviation, tenant_type, sample, percentage, active
FROM
  `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['citypatternsactivity_table'] }}`
ORDER BY city, state_abbreviation, tenant_type