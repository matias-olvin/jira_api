CREATE OR REPLACE TABLE
  `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['market_analysis_table'] }}`
AS


WITH

state_pois AS (
  SELECT a.pid, fk_sgbrands, long_lat_point
  FROM(
    SELECT pid, fk_sgbrands, ST_GEOGPOINT(longitude, latitude) as long_lat_point
    FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`
    WHERE fk_sgbrands IS NOT NULL
  ) a
  INNER JOIN `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['brands_table'] }}` b
    ON a.fk_sgbrands = b.pid
),

malls AS (
  SELECT pid, name, ST_GEOGPOINT(longitude, latitude) as long_lat_point, region as state
  FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`
  WHERE naics_code=531120
),

census_tracts AS(
  SELECT pid as Tract_FIPS, RUCA as primary_RUCA, polygon as tract_geom
  FROM `{{ params['storage-prod'] }}.{{ params['area_geometries_dataset'] }}.{{ params['census_table'] }}`
),
malls_RUCA AS(
  SELECT a.*, b.primary_RUCA
  FROM malls a
  INNER JOIN census_tracts b
    ON ST_WITHIN(a.long_lat_point, b.tract_geom)
),

void_all_malls AS(
  SELECT state, primary_RUCA, mall, fk_sgbrands, within_5mi
  FROM(
    SELECT mall, fk_sgbrands, count(*) as within_5mi
    FROM
      (SELECT a.fk_sgbrands, primary_RUCA, b.pid as mall
      FROM state_pois a
      INNER JOIN malls_RUCA b
        ON st_distance(a.long_lat_point, b.long_lat_point) < 8047
      )
    GROUP BY mall, fk_sgbrands
  ) a
  LEFT JOIN malls_RUCA b
    ON a.mall = b.pid
),

empty_table_structure AS(
  SELECT state, primary_RUCA, pid as fk_sgbrands, name as brands
  FROM `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['brands_table'] }}`
  FULL OUTER JOIN (
    SELECT state, RUCA as primary_RUCA
    FROM `{{ params['storage-prod'] }}.{{ params['area_geometries_dataset'] }}.{{ params['census_table'] }}`
    GROUP BY state, primary_RUCA
    )
    ON TRUE
)


SELECT state, primary_RUCA, fk_sgbrands, brands,
       ifnull(avg_within_5mi,0) as avg_within_5mi,
       ifnull(num_malls_within_5mi,0) as num_malls_within_5mi,
       num_malls_same_RUCA
FROM empty_table_structure
LEFT JOIN (
  SELECT state, primary_RUCA, fk_sgbrands,
          CASE count(nullif(within_5mi,0))
              WHEN 0 THEN 0
              ELSE SUM(within_5mi)/count(nullif(within_5mi,0))
              END as avg_within_5mi,
          count(nullif(within_5mi,0)) as num_malls_within_5mi
  FROM void_all_malls
  GROUP BY state, primary_RUCA, fk_sgbrands
)
  USING(state, primary_RUCA, fk_sgbrands)
LEFT JOIN (
  SELECT state, primary_RUCA, count(*) as num_malls_same_RUCA
  FROM malls_RUCA
  GROUP BY state, primary_RUCA
)
  USING(state, primary_RUCA)