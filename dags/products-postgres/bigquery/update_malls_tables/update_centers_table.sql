CREATE OR REPLACE TABLE
  `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGCenterRaw_table'] }}`
--  `storage-prod-olvin-com.postgres.SGCenterRaw`
--  `storage-prod-olvin-com.postgres_batch.SGCenterRaw`
AS


SELECT a.*,
      IFNULL(active_places_count,0) AS active_places_count,
      IF(IFNULL(active_places_count,0) > 0, TRUE, FALSE) AS monthly_visits_availability,
      IF(IFNULL(high_granularity_count,0) > 0, TRUE, FALSE) AS patterns_availability
FROM `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{ params['malls_base_table'] }}` a
LEFT JOIN (
  SELECT fk_parents,
         COUNTIF(activity IN ('active', 'limited_data')) AS active_places_count,
         COUNTIF(activity = 'active') AS high_granularity_count
  FROM (
    SELECT pid, fk_parents, activity
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
  GROUP BY fk_parents
)
ON a.pid = fk_parents