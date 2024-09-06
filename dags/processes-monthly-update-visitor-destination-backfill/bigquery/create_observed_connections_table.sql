WITH
places_table AS (
  SELECT fk_sgplaces, fk_sgbrands, b.name as brands, a.name
  FROM (
    SELECT pid as fk_sgplaces, fk_sgbrands, name
    FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`
--    FROM `storage-prod-olvin-com.postgres.SGPlaceRaw`
    WHERE fk_sgbrands is not null
  ) a
  LEFT JOIN `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['brands_table'] }}` b
--  LEFT JOIN `storage-prod-olvin-com.postgres.SGBrandRaw` b
    ON a.fk_sgbrands = b.pid
),

poi_visits_scaled AS (
  SELECT device_id, fk_sgplaces, visit_score
  FROM `{{ params['storage-prod'] }}.{{ params['visit_dataset'] }}.*`
--  FROM `storage-prod-olvin-com.poi_visits_scaled.*`
  WHERE visit_score > 0
        AND local_date >= DATE_TRUNC(DATE_SUB( CAST("{{ md_date_start }}" AS DATE), INTERVAL 1 MONTH), MONTH)
        AND local_date < DATE_TRUNC(DATE_SUB( CAST("{{ md_date_start }}" AS DATE), INTERVAL 0 MONTH), MONTH)
),

num_of_devices AS (
  SELECT fk_sgplaces, count(device_id) as total_devices
  FROM poi_visits_scaled
  INNER JOIN places_table
    USING(fk_sgplaces)
  GROUP BY fk_sgplaces
),

brands_visited_by_device AS(
  SELECT device_id, fk_sgbrands
  FROM poi_visits_scaled a
  INNER JOIN places_table b
    USING(fk_sgplaces)
)


SELECT a.*, b.total_devices,
       DATE_TRUNC(DATE_SUB( CAST("{{ md_date_start }}" AS DATE), INTERVAL 0 MONTH), MONTH) as local_date
FROM(
  SELECT b.fk_sgplaces, c.fk_sgbrands, count(distinct a.device_id) as shared_devices
  FROM poi_visits_scaled a -- visitors to our store
  INNER JOIN places_table b -- our store info
    USING(fk_sgplaces)
  INNER JOIN brands_visited_by_device c -- brands visited by those visitors (being of different brand)
    ON a.device_id = c.device_id AND c.fk_sgbrands <> b.fk_sgbrands
  GROUP BY b.fk_sgplaces, c.fk_sgbrands
) a
INNER JOIN num_of_devices b
  USING(fk_sgplaces)