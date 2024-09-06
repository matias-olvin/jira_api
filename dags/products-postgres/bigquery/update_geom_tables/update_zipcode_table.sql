CREATE OR REPLACE TABLE
  `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['zipcoderaw_table'] }}`
  -- `storage-prod-olvin-com.postgres.ZipCodeRaw`
  -- `storage-prod-olvin-com.postgres_batch.ZipCodeRaw`
AS

SELECT a.pid, a.primary_city, a.county, a.state, a.population, a.point, a.polygon,
      IFNULL(active_places_count,0) AS active_places_count,
      IF(IFNULL(active_places_count,0) > 0, TRUE, FALSE) AS monthly_visits_availability,
      IF(IFNULL(high_granularity_count,0) > 0, TRUE, FALSE) AS patterns_availability
FROM
  `{{ params['project'] }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}` a
  --`storage-prod-olvin-com.area_geometries.zipcodes` a
LEFT JOIN (
  SELECT zipcode,
         COUNTIF(activity IN ('active', 'limited_data')) AS active_places_count,
         COUNTIF(activity = 'active') AS high_granularity_count
  FROM (
    SELECT pid, CAST(postal_code AS INT64) as zipcode, city_id, activity
    FROM
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}`
      -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
      -- `storage-prod-olvin-com.postgres_batch.SGPlaceRaw`
    INNER JOIN
      `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['sgplaceactivity_table'] }}`
      -- `storage-prod-olvin-com.postgres.SGPlaceActivity`
      -- `storage-prod-olvin-com.postgres_batch.SGPlaceActivity`
      ON pid = fk_sgplaces
  )
  GROUP BY zipcode
)
ON a.pid = zipcode