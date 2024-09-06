CREATE OR REPLACE TABLE
  `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['citypatternsactivity_table'] }}`
--  `storage-prod-olvin-com.postgres.CityPatternsActivityRaw`
--  `storage-prod-olvin-com.postgres_batch.CityPatternsActivityRaw`
AS

WITH

coverage_data AS(
  SELECT city_id, tenant_type,
        COUNTIF(activity IN ('active', 'limited_data')) AS monthly_visits_count,
        COUNTIF(activity = 'active') AS hourly_visits_count
  FROM (
    SELECT pid, CAST(postal_code AS INT64) as zipcode, city_id, almanac_category AS tenant_type, activity
    FROM
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}`
      -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
      -- `storage-prod-olvin-com.postgres_batch.SGPlaceRaw`
    INNER JOIN
      `{{ params['project'] }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
--      `storage-prod-olvin-com.sg_base_tables.naics_code_subcategories`
      USING(naics_code)
    INNER JOIN
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['sgplaceactivity_table'] }}`
      -- `storage-prod-olvin-com.postgres.SGPlaceActivity`
      -- `storage-prod-olvin-com.postgres_batch.SGPlaceActivity`
      ON pid = fk_sgplaces
  )
  GROUP BY city_id, tenant_type
),

template_for_tenant_type AS(
  SELECT city_id, city, state_abbreviation, tenant_type
  FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['cityraw_table'] }}`
--    `storage-prod-olvin-com.postgres.CityRaw`
--    `storage-prod-olvin-com.postgres_batch.CityRaw`
  FULL OUTER JOIN (
    SELECT almanac_category AS tenant_type
    FROM
      `{{ params['project'] }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
--      `storage-prod-olvin-com.sg_base_tables.naics_code_subcategories`
    GROUP BY almanac_category
  )
  ON TRUE
),

coverage_tenants AS(
  SELECT city_id, city, state_abbreviation, tenant_type,
        IFNULL(hourly_visits_count, 0) AS sample,
        IFNULL(SAFE_DIVIDE(hourly_visits_count, monthly_visits_count), 0)*100 AS percentage,
        IFNULL(hourly_visits_count, 0) > 0 AS active
  FROM template_for_tenant_type
  LEFT JOIN coverage_data
  USING(city_id, tenant_type)
),

coverage_all AS(
  SELECT city_id, city, state_abbreviation, "All" AS tenant_type,
        IFNULL(hourly_visits_count, 0) AS sample,
        IFNULL(SAFE_DIVIDE(hourly_visits_count, monthly_visits_count), 0)*100 AS percentage,
        IFNULL(hourly_visits_count, 0) > 0 AS active
  FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['cityraw_table'] }}`
--    `storage-prod-olvin-com.postgres.CityRaw`
--    `storage-prod-olvin-com.postgres_batch.CityRaw`
  LEFT JOIN (
    SELECT city_id,
            SUM(monthly_visits_count) AS monthly_visits_count,
            SUM(hourly_visits_count) AS hourly_visits_count
    FROM coverage_data
    GROUP BY city_id
  )
  USING(city_id)
)

SELECT *
FROM coverage_all
UNION ALL
SELECT *
FROM coverage_tenants