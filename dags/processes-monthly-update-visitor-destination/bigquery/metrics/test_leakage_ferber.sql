--CREATE OR REPLACE TABLE
--    `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_metrics_dataset'] }}.{{ params['monitoring_leakage_table']}}`
--AS

WITH
nearby_stores AS(
  SELECT pid as fk_sgplaces, name, fk_sgbrands
  FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`
  -- `storage-prod-olvin-com.postgres.SGPlaceRaw`
  WHERE ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ST_GEOGPOINT(-82.486745,28.893944)) < 1609 --less than 10 mi
),


--  LEAKAGE/SYNERGY
Aux_Grouped_Table AS(
    SELECT  a.fk_sgbrands, shared_devices, total_devices
    FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}` a
    -- `storage-prod-olvin-com.visitor_brand_destinations.observed_connections` a
    INNER JOIN nearby_stores b
      USING(fk_sgplaces)
),


to_validate AS(
  SELECT b.name as brands, a.*,
         a.devices_shared/total_devices as percentage_devices, total_devices,
         row_number() over(order by devices_shared desc) as idx
  FROM(
    SELECT fk_sgbrands, SUM(shared_devices) as devices_shared
    FROM Aux_Grouped_Table
    GROUP BY fk_sgbrands
    ) a
  LEFT JOIN(
    SELECT sum(total_devices) as total_devices
    FROM(
      SELECT fk_sgplaces, any_value(total_devices) as total_devices
      FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}`
      -- `storage-prod-olvin-com.visitor_brand_destinations.observed_connections`
      GROUP BY fk_sgplaces) a
    INNER JOIN nearby_stores b
      USING(fk_sgplaces)
  )
    ON TRUE
  LEFT JOIN `storage-prod-olvin-com.postgres.SGBrandRaw` b
    ON a.fk_sgbrands = b.pid
  ORDER BY devices_shared DESC
),

ref_table AS(
SELECT *, row_number() over(order by devices_shared desc) as idx
FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_metrics_dataset'] }}.{{ params['ferber_leakage_table'] }}`
-- `storage-prod-olvin-com.visitor_brand_destinations_metrics.ferber_leakage_ref`
)

SELECT DATE('{{ds}}') as run_date, *,
        IF(IF(  lost_10>1,  1,0) + IF(  lost_20>2,  1,0) + IF(  lost_30>3,  1,0) + IF(  lost_50>5,  1,0)>0, True, False) -- warn based on any subsect loosing 10% of pois
     OR IF(IF(factor_10>1.5,1,0) + IF(factor_20>1.5,1,0) + IF(factor_30>1.5,1,0) + IF(factor_50>1.5,1,0)>0, True, False) -- warn based on any factor over 1.5
     as warning,
        IF(IF(  lost_10>1,  1,0) + IF(  lost_20>2,  1,0) + IF(  lost_30>3,  1,0) + IF(  lost_50>5,  1,0)>2, True, False) -- error based on >2 subsects loosing 10% of pois
     OR IF(IF(  lost_10>2,  1,0) + IF(  lost_20>4,  1,0) + IF(  lost_30>6,  1,0) + IF(  lost_50>10,  1,0)>0, True, False) -- warn based on any subsect loosing 20% of pois
     OR IF(IF(factor_10>1.5,1,0) + IF(factor_20>1.5,1,0) + IF(factor_30>1.5,1,0) + IF(factor_50>1.5,1,0)>2, True, False) -- warn based on >2 factors over 1.5
     OR IF(IF(factor_10>2  ,1,0) + IF(factor_20>2  ,1,0) + IF(factor_30>2  ,1,0) + IF(factor_50>2  ,1,0)>0, True, False) -- warn based on any factor over 2
    as terminate
FROM(
  SELECT 10-count(*) as lost_10, --(sum(idx)-max(idx))/(count(*)-1)  as validate_10,
         (sum(idx)-max(idx))/(count(*)-1) / (count(*)/2) as factor_10
  FROM to_validate
  INNER JOIN (
    SELECT fk_sgbrands
    FROM ref_table a
    WHERE idx <= 10
  )
  USING(fk_sgbrands)
)
LEFT JOIN(
  SELECT 20-count(*) as lost_20, --(sum(idx)-max(idx))/(count(*)-1)  as validate_20,
         (sum(idx)-max(idx))/(count(*)-1) / (count(*)/2) as factor_20
  FROM to_validate
  INNER JOIN (
    SELECT fk_sgbrands
    FROM ref_table a
    WHERE idx <= 20
  )
  USING(fk_sgbrands)
)
  ON True
LEFT JOIN(
  SELECT 30-count(*) as lost_30, --(sum(idx)-max(idx))/(count(*)-1)  as validate_30,
         (sum(idx)-max(idx))/(count(*)-1) / (count(*)/2) as factor_30
  FROM to_validate
  INNER JOIN (
    SELECT fk_sgbrands
    FROM ref_table a
    WHERE idx <= 30
  )
  USING(fk_sgbrands)
)
  ON True
LEFT JOIN(
  SELECT 50-count(*) as lost_50, --(sum(idx)-max(idx))/(count(*)-1)  as validate_50,
         (sum(idx)-max(idx))/(count(*)-1) / (count(*)/2) as factor_50
  FROM to_validate
  INNER JOIN (
    SELECT fk_sgbrands
    FROM ref_table a
    WHERE idx <= 50
  )
  USING(fk_sgbrands)
)
  ON True
