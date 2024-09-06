CREATE OR REPLACE TABLE
  `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['cityraw_table'] }}`
  -- `storage-prod-olvin-com.postgres.CityRaw`
  -- `storage-prod-olvin-com.postgres_batch.CityRaw`
AS

SELECT a.*,
      IFNULL(active_places_count,0) AS active_places_count,
      IF(IFNULL(active_places_count,0) > 0, TRUE, FALSE) AS monthly_visits_availability,
      IF(IFNULL(high_granularity_count,0) > 0, TRUE, FALSE) AS patterns_availability,
      IFNULL(zipcodes,'{}') AS zipcodes
FROM
  `{{ params['project'] }}.{{ params['area_geometries_dataset'] }}.{{ params['city_table'] }}` a
  -- `storage-prod-olvin-com.area_geometries.city` a
LEFT JOIN (
  SELECT city_id,
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
  GROUP BY city_id
)
USING(city_id)
LEFT JOIN (
  SELECT city_id, TO_JSON_STRING(ARRAY_AGG(pid)) as zipcodes
  FROM
    `{{ params['project'] }}.{{ params['area_geometries_dataset'] }}.{{ params['city_table'] }}` b,
    `{{ params['project'] }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}` c
    -- `storage-prod-olvin-com.area_geometries.city` b,
    -- `storage-prod-olvin-com.area_geometries.zipcodes` c
  WHERE ST_INTERSECTS(b.polygon, ST_GEOGFROMTEXT(c.polygon))
  GROUP BY city_id
)
USING(city_id)