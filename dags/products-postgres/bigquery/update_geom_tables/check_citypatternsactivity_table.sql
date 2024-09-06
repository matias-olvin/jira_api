
SELECT max(error_)
FROM(
  SELECT city, state_abbreviation, all_sample - tenants_sample AS error_
  FROM (
    SELECT city, state_abbreviation, SUM(sample) as tenants_sample
    FROM
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['citypatternsactivity_table'] }}`
--    `storage-prod-olvin-com.postgres.CityPatternsActivity`
--    `storage-prod-olvin-com.postgres_batch.CityPatternsActivity`
    WHERE tenant_type<>"All"
    GROUP BY city, state_abbreviation
  )
  INNER JOIN(
    SELECT city, state_abbreviation, SUM(sample) as all_sample
    FROM
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['citypatternsactivity_table'] }}`
--    `storage-prod-olvin-com.postgres.CityPatternsActivityRaw`
--    `storage-prod-olvin-com.postgres_batch.CityPatternsActivityRaw`
    WHERE tenant_type="All"
    GROUP BY city, state_abbreviation
  )
  USING (city, state_abbreviation)
)