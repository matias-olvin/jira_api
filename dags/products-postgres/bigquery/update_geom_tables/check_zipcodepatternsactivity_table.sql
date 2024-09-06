SELECT max(error_)
FROM(
  SELECT zipcode, all_sample - tenants_sample AS error_
  FROM (
    SELECT zipcode, SUM(sample) as tenants_sample
    FROM
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['zipcodepatternsactivity_table'] }}`
--    `storage-prod-olvin-com.postgres.ZipCodePatternsActivityRaw`
--    `storage-prod-olvin-com.postgres_batch.ZipCodePatternsActivityRaw`
    WHERE tenant_type<>"All"
    GROUP BY zipcode
  )
  INNER JOIN(
    SELECT zipcode, SUM(sample) as all_sample
    FROM
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['zipcodepatternsactivity_table'] }}`
--    `storage-prod-olvin-com.postgres.ZipCodePatternsActivityRaw`
--    `storage-prod-olvin-com.postgres_batch.ZipCodePatternsActivityRaw`
    WHERE tenant_type="All"
    GROUP BY zipcode
  )
  USING (zipcode)
)