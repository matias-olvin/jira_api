  -- TO BE DONE FOR SECOND VERSION - Adding the centroid, chains... representation
CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['filtering_places_table'] }}` AS
WITH
  -- places
  places AS (
  SELECT
    * except (pid),
    pid as fk_sgplaces
  FROM
    --`storage-prod-olvin-com.sg_places.20211101`
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  where iso_country_code = 'US' ),
  footprints_places AS (
  SELECT
    pid AS fk_sgplaces,
    SAFE.ST_GEOGPOINT(longitude, latitude) as point,
    ST_ASTEXT(polygon_wkt) as polygon_wkt
  FROM
    --`storage-prod-olvin-com.sg_places.20211101`
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
    where iso_country_code = 'US' ),
  -- places with unique footprints
  places_unique_footprint AS (
  SELECT
    footprints_places.fk_sgplaces as fk_sgplaces,
    footprints_places.point,
    SAFE.ST_GEOGFROMTEXT(footprints_places.polygon_wkt) as polygon
  FROM
    footprints_places
  INNER JOIN
    (   SELECT
            polygon_wkt,
            COUNT(*) AS count,
        FROM
            footprints_places
        GROUP BY
            polygon_wkt
    )  as footprints_places_count
  USING
    (polygon_wkt)
  WHERE
    footprints_places_count.count = 1 ),
  -- copy of places with unique footprints
  places_unique_footprint_copy AS (
  SELECT
    fk_sgplaces AS fk_sgplaces_copy,
    point,
    polygon
  FROM
    places_unique_footprint ),
  -- join based on distance, not overlapping and not being the same polygon
  join_table AS (
  SELECT
    fk_sgplaces,
    fk_sgplaces_copy,
  FROM
    places_unique_footprint
  INNER JOIN
    places_unique_footprint_copy
  ON
    ST_DWITHIN(places_unique_footprint.point,
      places_unique_footprint_copy.point,
      20)
    AND NOT ST_INTERSECTS(places_unique_footprint.polygon,
      places_unique_footprint_copy.polygon )
    AND NOT places_unique_footprint.fk_sgplaces = places_unique_footprint_copy.fk_sgplaces_copy ),
  -- only couples
  join_table_couples as (
      select * except(count_neighbours)
      from join_table
      inner join (select fk_sgplaces, count(*) as count_neighbours from join_table group by fk_sgplaces ) as count_neighbour_table
      using (fk_sgplaces)
      where count_neighbour_table.count_neighbours = 1
  ),
  -- distict footprints
    distinct_pid AS(
  SELECT
    DISTINCT fk_sgplaces
  FROM
    join_table_couples ),
  -- FROM THIS POINT ON WE PREPARE TWO DATASETS
  -- 1ST DATASET(final_footprints): POLYGONS, TO SEE IN KEPLER AND CHECK IF WE LIKE THE 20METERS THRESHOLD
  -- we add the corresponding footprint, as places dont share polygon distinct_pid and train_pois have the same rows
  footprints_to_plot AS (
  SELECT
    *,
  FROM
    distinct_pid
  LEFT JOIN
    places
  USING (fk_sgplaces)),
  -- 2ND DATASET(groups_final): DATA USE TO TRAIN -  EACH POI WITH THE NEIGHBOUR POIS
  groups_train AS (
  SELECT
    *
  FROM
    distinct_pid
  LEFT JOIN
    join_table_couples
  using
    (fk_sgplaces) ),
  groups_train_attributes AS (
  SELECT
    fk_sgplaces,
    fk_sgplaces_copy as fk_sgplaces_neigh,
    categories_match_table_old.olvin_category as neighbour_category_old,
    categories_match_table_new.olvin_category as neighbour_category
  FROM
    groups_train
  LEFT JOIN
    --`storage-prod-olvin-com.sg_places.20211101` AS places_table
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` AS places_table
  ON
    groups_train.fk_sgplaces_copy = places_table.pid
  LEFT JOIN
    (select naics_code, olvin_category from
        --`storage-prod-olvin-com.sg_base_tables.sg_categories_match`
        `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['categories_match_table'] }}`
    ) AS categories_match_table_old
  ON CAST(places_table.naics_code as string) = categories_match_table_old.naics_code
  LEFT JOIN
    (select naics_code, olvin_category from
        --`storage-prod-olvin-com.sg_base_tables.naics_code_subcategories`
        `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
        ) AS categories_match_table_new
  ON places_table.naics_code  = categories_match_table_new.naics_code
  where iso_country_code = 'US'
    )
SELECT
  *
FROM
  groups_train_attributes