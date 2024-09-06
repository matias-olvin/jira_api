CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_regressors_staging_dataset'] }}.{{ params['covid_restrictions_dictionary_table'] }}` AS
  --Assigning a poi to its relevant city, county and state
WITH
  poi_attributes AS (
    SELECT
      primary_city,
      county,
      state,
      ST_UNION_AGG((ST_GEOGFROMTEXT(polygon))) AS union_polygon,
    FROM
      `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
    GROUP BY
      primary_city,
      county,
      state
  ),
  -- Taking all pois and its "geopoint" we have
  all_pois AS (
    SELECT
      pid AS fk_sgplaces,
      name,
      ST_GEOGPOINT(longitude, latitude) AS poi_point,
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  ),
  -- Assigning the pois to its relevant city,county,state
  pois_assigned AS (
    SELECT
      all_pois.fk_sgplaces,
      poi_attributes.primary_city,
      poi_attributes.county,
      poi_attributes.state,
    FROM
      all_pois
      JOIN poi_attributes ON ST_WITHIN(all_pois.poi_point, poi_attributes.union_polygon)
  ),
  -- Joining attributes
  combined_poi AS (
    SELECT
      *
    FROM
      all_pois
      LEFT JOIN pois_assigned USING (fk_sgplaces)
  ),
  original_table AS (
    SELECT
      *
    FROM
      combined_poi
  ),
  --POIs that returned their locations as null we need to assign a
  pois_without_shape AS (
    SELECT
      fk_sgplaces,
      name,
      poi_point
    FROM
      original_table
    WHERE
      state IS NULL
  ),
  -- All combinations of distances between polygons and POIs
  all_combinations_table AS (
    SELECT
      fk_sgplaces,
      name,
      poi_point,
      primary_city,
      county,
      state,
      ST_DISTANCE(poi_point, union_polygon) AS distance_geoms
    FROM
      pois_without_shape
      CROSS JOIN poi_attributes
  ),
  -- minimum distance of each POI
  min_distance AS (
    SELECT
      fk_sgplaces,
      MIN(distance_geoms) AS distance_geoms
    FROM
      all_combinations_table
    GROUP BY
      fk_sgplaces
  ),
  -- POIs assignation of primary_city, county and state
  final_table AS (
    SELECT
      * EXCEPT (distance_geoms)
    FROM
      all_combinations_table
      INNER JOIN min_distance USING (fk_sgplaces, distance_geoms)
  ),
  complete_final_table AS (
    SELECT
      *
    FROM
      final_table
    UNION ALL
    SELECT
      *
    FROM
      original_table
    WHERE
      state IS NOT NULL
  ),
  -- POIs and their naics_code
  naics_column AS (
    SELECT
      pid,
      naics_code,
      CAST(SUBSTR(CAST(naics_code AS STRING), 0, 4) AS INT64) AS first_four_digits_naics_code
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  ),
  final_poi_table_naics AS (
    SELECT
      * EXCEPT (pid)
    FROM
      complete_final_table
      INNER JOIN naics_column ON complete_final_table.fk_sgplaces = naics_column.pid
  ),
  --Assinging POIs with the covid restricitons category code
  pois_with_general_code_final AS (
    SELECT
      *,
      IFNULL(
        CASE naics_code
          WHEN 445110 THEN '445110'
          WHEN 445120 THEN '445120'
          WHEN 444130 THEN '444130'
          WHEN 713940 THEN '713940'
          WHEN 446110 THEN '446110'
          WHEN 452311 THEN '452311'
          ELSE NULL
        END,
        CASE first_four_digits_naics_code
          WHEN 7224 THEN '7224'
          WHEN 7211 THEN '7211'
          WHEN 5221 THEN '5221'
          WHEN 6221 THEN '6221'
          WHEN 4471 THEN '4471'
          WHEN 4413 THEN '4413'
          WHEN 8123 THEN '8123'
          WHEN 7225 THEN '7225'
          WHEN 7111 THEN '7111'
          WHEN 8121 THEN '8121'
          ELSE "44_45"
        END
      ) AS general_code
    FROM
      final_poi_table_naics
  )
SELECT
  * EXCEPT (name, poi_point, naics_code)
FROM
  pois_with_general_code_final;