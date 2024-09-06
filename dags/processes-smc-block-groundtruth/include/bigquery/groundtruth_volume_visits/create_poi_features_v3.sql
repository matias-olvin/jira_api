CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_v3_table'] }}`
AS


WITH
-- Table with all the pois for which we want to create features
pois_list AS (
  SELECT pid AS fk_sgplaces, fk_sgbrands, ST_GEOGPOINT(longitude, latitude) as location, postal_code, latitude
  FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset']}}.{{ params['sgplaceraw_table']}}`
  WHERE fk_sgbrands is not null
),

-- Population features
  --  Get population at each block from census
  blocks_with_population AS(
    SELECT block_id, state_fips_code, population, internal_point
    FROM `{{ var.value.env_project }}.{{ params['area_geometries_dataset']}}.{{ params['census_blocks_table']}}`
    WHERE population > 0
  ),

  --  Agg blocks population when they are within desired distance
  distance_features AS (
  SELECT fk_sgplaces,
        SUM(population) AS population_25km,
        SUM(CASE WHEN ST_DISTANCE(location, internal_point) < 10000 THEN population
                  ELSE 0
                  END) AS population_10km,
        SUM(CASE WHEN ST_DISTANCE(location, internal_point) < 2500 THEN population
                  ELSE 0
                  END) AS population_2_5km,
        SUM(CASE WHEN ST_DISTANCE(location, internal_point) < 1000 THEN population
                  ELSE 0
                  END) AS population_1km,
        SUM(CASE WHEN ST_DISTANCE(location, internal_point) < 500 THEN population
                  ELSE 0
                  END) AS population_500m
  FROM pois_list
  INNER JOIN blocks_with_population
    ON ST_DISTANCE(location, internal_point) < 25000
  GROUP BY fk_sgplaces
    ),

-- Additional geographic features: RUCA and whether a poi is at coastal zipcode
  aux_geo_features AS(
  SELECT fk_sgplaces,
        IFNULL(RUCA,0) AS RUCA,
        IFNULL(is_coast, FALSE) AS is_coast,
        latitude
  FROM pois_list
  LEFT JOIN(
    SELECT fk_sgplaces, RUCA
    FROM pois_list
    INNER JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset']}}.{{ params['census_tracts_table']}}`
    ON ST_WITHIN(location, polygon)
  )
  USING(fk_sgplaces)
  LEFT JOIN (
    SELECT pid AS zipcode, is_coast
    FROM `{{ var.value.env_project }}.{{ params['area_geometries_dataset']}}.{{ params['zipcodes_table']}}`
  )
  ON CAST(postal_code AS INT64) = zipcode
  ),

-- Alternate source features: Data For SEO
  seo_features AS(
    SELECT fk_sgplaces,
            AVG(rating) AS rating,
            MAX(seo_count) AS seo_count,
            AVG(delta_rating) AS delta_rating,
            MAX(delta_seo_count) AS delta_seo_count,
            MAX(num_photos) AS num_photos,
            AVG(popular_index_sum) AS popular_index_sum
    FROM pois_list
    INNER JOIN `{{ var.value.env_project }}.{{ params['static_features_dataset']}}.{{ params['dataforseo_table']}}`
    USING(fk_sgplaces)
    GROUP BY fk_sgplaces
  ),

-- Include prior info brand/category based visits as feature
  prior_visits_features AS(
    SELECT fk_sgplaces, IFNULL(median_brand_visits, median_category_visits) AS prior_volume_visits,
           CASE WHEN median_brand_visits IS NULL
                THEN sub_category
                ELSE fk_sgbrands
                END AS prior_volume_category
    FROM pois_list
    INNER JOIN `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset']}}.{{ params['prior_brand_visits_table']}}`
    USING(fk_sgbrands)
  ),


-- S2 Cell extrapolation of ground truth info
  -- get pois with ground truth visits
  gtvm_target AS (
    SELECT fk_sgplaces, fk_sgbrands, location, visits_per_day
    FROM `{{ params['sns_project']}}.{{ params['accessible_by_olvin_dataset']}}.v-{{ params['smc_gtvm_dataset']}}-{{ params['gtvm_target_agg_sns_table']}}`
    INNER JOIN pois_list
    USING(fk_sgplaces)
  ),

  num_pois_brand AS(
    SELECT fk_sgbrands, COUNT(*) AS num_pois
    FROM gtvm_target
    GROUP BY fk_sgbrands
  ),

  --  compute the ranking of each poi with ground truth visits within its brand
  place_ranks AS (
    SELECT
      fk_sgplaces,
      fk_sgbrands,
      RANK() OVER (PARTITION BY fk_sgbrands ORDER BY visits_per_day) AS place_order
    FROM
      gtvm_target
  ),

  merged_table AS(
    SELECT fk_sgplaces, location, fk_sgbrands, visits_per_day, num_pois, place_order, 100*place_order/(num_pois+1) AS approx_perc
    FROM place_ranks
    INNER JOIN num_pois_brand
      USING(fk_sgbrands)
    INNER JOIN gtvm_target
      USING(fk_sgplaces, fk_sgbrands)
    ORDER BY fk_sgbrands, visits_per_day DESC
  ),

  -- Assign each poi in pois_list all s2 cells it belongs from level 0 to 15
  geoloc_target AS (
    SELECT fk_sgplaces,
      TO_HEX(
        CAST(
          (
            -- We want the final result in hexadecimal
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 0) >> bit & 0x1 AS STRING --location are coordinates
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
          ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string
        )
      ) AS source_s2_token_level_0, --S2 token is created from pid location
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 1) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_1,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 2) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_2,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 3) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_3,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 4) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_4,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 5) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_5,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 6) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_6,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 7) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_7,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 8) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_8,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 9) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_9,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 10) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_10,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 11) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_11,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 12) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_12,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 13) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_13,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 14) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_14,
      TO_HEX(
        CAST(
          (
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(location, 15) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST(GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS source_s2_token_level_15,
    FROM pois_list
  ),

  -- Assing each s2 cell with >=5 ground truth - pois inside the agg info of those pois --> extrapolation of gt info
  s2_cells_perc_sns_agg AS (
    SELECT
      source_s2_token AS s2_token,
      s2_level,
      SUM(visits_per_day) AS ground_truth_visits,
      AVG(approx_perc) AS avg_perc,
      SUM(approx_perc * num_pois) / SUM(num_pois) AS avg_weighted_perc,
      COUNT(*) AS num_pois_within_s2
    FROM(
      SELECT
        TO_HEX(
          CAST(
            (
              SELECT
                STRING_AGG(
                  CAST(
                    S2_CELLIDFROMPOINT(location, s2_level) >> bit & 0x1 AS STRING
                  ),
                  ''
                  ORDER BY
                    bit DESC
                )
              FROM
                UNNEST(GENERATE_ARRAY(0, 63)) AS bit
            ) AS BYTES FORMAT "BASE2"
          )
        ) AS source_s2_token,
        s2_level, num_pois, approx_perc, visits_per_day
      FROM merged_table
      FULL OUTER JOIN (
        SELECT  s2_level
        FROM UNNEST(GENERATE_ARRAY(0, 15)) AS s2_level
      )
        ON True
      WHERE location IS NOT NULL
    )
    GROUP BY source_s2_token, s2_level
    HAVING num_pois_within_s2 >= 5
  ),

  -- Assign each poi in poi_list the info of the s2cells where it's contained which have extrapolated gt info
  base_table_s2_assign AS(
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_15
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_14
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_13
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_12
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_11
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_10
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_9
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_8
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_7
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_6
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_5
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_4
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_3
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_2
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_1
    )
    UNION ALL
    (
      SELECT fk_sgplaces, s2_level, s2_token,
            ground_truth_visits AS sum_gt_traffic_s2_cell,
            avg_perc, avg_weighted_perc
      FROM geoloc_target
      INNER JOIN
        s2_cells_perc_sns_agg
        ON s2_token = source_s2_token_level_0
    )
  ),

  --  Keep only the most granular s2cell with info for each poi
  s2cell_gt_features AS(
    SELECT fk_sgplaces, s2_level, sum_gt_traffic_s2_cell,
           avg_perc AS avg_percentile_s2_cell,
           avg_weighted_perc AS avg_weighted_percentile_s2_cell
    FROM(
      SELECT fk_sgplaces, max(s2_level) AS s2_level
      FROM base_table_s2_assign
      GROUP BY fk_sgplaces
    )
    INNER JOIN base_table_s2_assign
      USING(fk_sgplaces, s2_level)
  ),


-- Get CBSA & State population features
cbsa_per_poi AS(
  SELECT pid AS fk_sgplaces, fk_sgbrands, region, cbsa_fips_code
  FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset']}}.{{ params['sgplaceraw_table']}}`
  LEFT JOIN(
    SELECT fk_sgplaces, cbsa_fips_code
    FROM(
      SELECT pid AS fk_sgplaces, ST_GEOGPOINT(longitude, latitude) pt_coords
      FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset']}}.{{ params['sgplaceraw_table']}}`
    )
    INNER JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset']}}.{{ params['cbsa_boundaries_table']}}`
      ON ST_WITHIN(pt_coords, cbsa_geom)
  )
  ON pid=fk_sgplaces
),

cbsa_population_table AS(
  SELECT cbsa_fips_code, CAST(SUM(population) AS FLOAT64) AS cbsa_population
  FROM(
    SELECT cbsa_fips_code, population
    FROM(
      SELECT population, polygon AS block_polygon
      FROM `{{ var.value.env_project }}.{{ params['area_geometries_dataset']}}.{{ params['census_blocks_table']}}`
    )
    INNER JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset']}}.{{ params['cbsa_boundaries_table']}}`
    ON ST_INTERSECTS(block_polygon, cbsa_geom)
  )
  GROUP BY 1
  ORDER BY 1
),

num_stores_per_cbsa AS(
  SELECT cbsa_fips_code, fk_sgbrands, COUNT(*)  AS num_stores
  FROM cbsa_per_poi
  WHERE fk_sgbrands IS NOT NULL
  GROUP BY cbsa_fips_code, fk_sgbrands
),

state_population_table AS(
  SELECT region, SUM(population) AS state_population
  FROM `{{ var.value.env_project }}.{{ params['area_geometries_dataset']}}.{{ params['census_blocks_table']}}`
  GROUP BY region
),

cbsa_population_features AS(
  SELECT
    fk_sgplaces,
    state_population,
    cbsa_population,
    num_stores AS num_stores_brand_cbsa,
    SAFE_DIVIDE(cbsa_population, num_stores) AS pop_per_store_cbsa
  FROM cbsa_per_poi
  LEFT JOIN state_population_table
    USING(region)
  LEFT JOIN cbsa_population_table
    USING(cbsa_fips_code)
  LEFT JOIN num_stores_per_cbsa
    USING(cbsa_fips_code, fk_sgbrands)
  WHERE fk_sgplaces IN(
    SELECT pid
    FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset']}}.{{ params['sgplaceraw_table']}}`
    WHERE fk_sgbrands IS NOT NULL
  )
)


-- Put all the features together adjoining them to previous step created table
SELECT *
FROM `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_2_table'] }}`
LEFT JOIN distance_features
USING(fk_sgplaces)
LEFT JOIN aux_geo_features
USING(fk_sgplaces)
LEFT JOIN seo_features
USING(fk_sgplaces)
LEFT JOIN s2cell_gt_features
USING(fk_sgplaces)
LEFT JOIN prior_visits_features
USING(fk_sgplaces)
LEFT JOIN cbsa_population_features
USING(fk_sgplaces)