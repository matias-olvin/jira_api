-- create or replace table `olvin-sandbox-20210203-0azu4n.ella_test.all_pois_stage_2` as 
BEGIN
CREATE temp TABLE temp_base_table_temp AS 
  SELECT
    *
  FROM
    -- `storage-prod-olvin-com.smc_poi_visits_scaled_block_daily_estimation.*`
      `{{ var.value.env_project }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}.*`
  LEFT JOIN (
    SELECT
      device_id,
      fk_zipcodes,
    FROM (
      SELECT
        device_id,
        fk_zipcodes,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY local_date DESC) AS row_num
      FROM
        -- `storage-prod-olvin-com.device_zipcodes.*`
        `{{ var.value.env_project }}.{{ params['device_zipcodes_dataset'] }}.*`
      WHERE
        local_date >= '2019-01-01' )
    WHERE
      row_num = 1 )
  USING
    (device_id)
  LEFT JOIN (
    SELECT
      pid AS fk_zipcodes,
      population
    FROM
      -- `storage-prod-olvin-com.area_geometries.zipcodes`
      `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`
      )
  USING
    (fk_zipcodes)
  WHERE
    local_date >= '2019-01-01' ;
END;

-- 2_base_table
BEGIN
CREATE temp TABLE temp_base_table as
with
visits_per_category AS (
  SELECT
    DISTINCT ifnull(naics_code, 1) naics_code,
     ifnull(avg_visits_score_daily_estimation_based_by_category,
      avg_visits_score_daily_estimation) avg_visits_by_category_daily_estimation,
  FROM (
    SELECT
      DISTINCT ifnull(naics_code, 1) naics_code,
      AVG(visit_score_steps.daily_estimation) OVER (PARTITION BY naics_code) AS avg_visits_score_daily_estimation_based_by_category,
      AVG(visit_score_steps.daily_estimation) OVER () AS avg_visits_score_daily_estimation
    FROM
      temp_base_table_temp  )
  ),
  fill_na AS (
  SELECT
    a.* EXCEPT(naics_code),
    nullif(naics_code, 1) naics_code,
    avg_visits_by_category_daily_estimation
  FROM (
    SELECT
      * EXCEPT(naics_code),
      ifnull(naics_code, 1) naics_code
    FROM
      temp_base_table_temp ) a
  LEFT JOIN
    visits_per_category b
  USING
    (naics_code) ),
  base_table AS (
  SELECT
    * EXCEPT(visit_score_steps),
    STRUCT(visit_score_steps.original AS original,
      visit_score_steps.weighted AS weighted,
      visit_score_steps.opening AS opening,
      visit_score_steps.visit_share AS visit_share,
      visit_score_real_daily_estimation AS daily_estimation) AS visit_score_steps
  FROM (
    SELECT
      *,
      ifnull(visit_score_steps.daily_estimation, avg_visits_by_category_daily_estimation) visit_score_real_daily_estimation
    FROM
      fill_na) )
  select * from base_table;
END;
BEGIN
CREATE temp TABLE temp_distances_to_closer_city as 
SELECT
    *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY fk_sgplaces ORDER BY distance_to_centre_city) AS row_num
    FROM (
      SELECT
        fk_sgplaces,
        b.fk_met_areas,
        b.name,
        ST_DISTANCE(long_lat_pl,
          ST_GEOGPOINT(b.longitude,
            b.latitude) ) AS distance_to_centre_city
      FROM
        -- `storage-prod-olvin-com.smc_ground_truth_volume_model.model_input_1` a
        `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_1_table'] }}` a
      CROSS JOIN
        -- `olvin-sandbox-20210203-0azu4n.rankings.centre_cities` b
              `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['centre_cities_table'] }}` b
        ) )
  WHERE
    row_num = 1;
END;
BEGIN
CREATE temp TABLE temp_fill_nas as 
with
-- 3 corresponding zip population, 
visits_from_corresponding_zip_pop as (
SELECT *
   FROM (
      SELECT
        fk_zipcodes,
        fk_sgplaces,
        population,
        SUM(visit_score_steps.daily_estimation) AS visits_from_corresponding_zipcode_population -- count(distinct device_id) // 1
      FROM
        temp_base_table
      GROUP BY
        fk_zipcodes,
        fk_sgplaces,
        population

)),
-- 4 pop_visiting
visits_to_POI_from_zipcode_home_table AS (
  SELECT
    fk_zipcodes,
    fk_sgplaces,
    population,
    SUM(visit_score_steps.daily_estimation) AS visits_to_POI_from_zipcode_home,
    COUNT(DISTINCT device_id) AS unique_devices_to_POI_from_zipcode_home,
    1 AS at_leat_one_visit_to_POI_from_zipcode_home
  FROM
    temp_base_table 
  GROUP BY
    fk_zipcodes,
    fk_sgplaces,
    population),
  adding_total_visits_from_zipcode AS (
  SELECT
    *,
    population * visits_to_POI_from_zipcode_home / nullif(SUM(visits_to_POI_from_zipcode_home) OVER (PARTITION BY fk_zipcodes),
      0 )AS population_share_of_zipcode_visits,
    population * unique_devices_to_POI_from_zipcode_home / nullif(SUM(unique_devices_to_POI_from_zipcode_home) OVER (PARTITION BY fk_zipcodes),
      0 )AS population_share_of_zipcode_unique_devices,
    population * at_leat_one_visit_to_POI_from_zipcode_home / nullif(SUM(at_leat_one_visit_to_POI_from_zipcode_home) OVER (PARTITION BY fk_zipcodes),
      0 )AS population_share_of_zipcode_at_least_one_appearance,
  FROM
    visits_to_POI_from_zipcode_home_table ),
 population_visiting as (
   SELECT
      fk_sgplaces,
      SUM(population_share_of_zipcode_visits) AS population_visiting,
      SUM(population_share_of_zipcode_unique_devices) AS population_visiting_unique_device,
      SUM(population_share_of_zipcode_at_least_one_appearance) AS population_visiting_equal_share,
    FROM
      adding_total_visits_from_zipcode
    GROUP BY
      fk_sgplaces 
 
 ),
get_visits_per_poi AS (
  SELECT
    fk_sgplaces,
    SUM(visit_score_steps.daily_estimation) visit_score_poi,
    SUM(visit_score_steps.original) AS almanac_observed_visits_original,
    SUM(visit_score_steps.visit_share) AS almanac_observed_visits_visit_share,
    SUM(visit_score_steps.daily_estimation) AS almanac_observed_visits
  FROM
    temp_base_table
  GROUP BY
    fk_sgplaces ),
-- 5  
join_lat_long_pl AS (
  SELECT
    a.*,
    b.long_lat_pl
  FROM
    get_visits_per_poi  a
  LEFT JOIN
       `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_1_table'] }}` b
    -- `storage-prod-olvin-com.smc_ground_truth_volume_model.model_input_1` b
  USING
    (fk_sgplaces) ),
  nearby_points AS (
  SELECT
    b.fk_sgplaces,
    visit_score_poi,
    a.fk_sgplaces AS fk_pois_neighbours
  FROM
    join_lat_long_pl a
  INNER JOIN
       `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_1_table'] }}` b
    -- `storage-prod-olvin-com.smc_ground_truth_volume_model.model_input_1` b
  ON
    ST_DISTANCE(a.long_lat_pl,
      b.long_lat_pl) <= 400 ),
 get_visits_per_point_big_radius AS (
  SELECT
    fk_sgplaces,
    SUM(visit_score_poi) visit_score_big_radius,
    COUNT(DISTINCT fk_pois_neighbours) AS count_poi_neighs_400
  FROM
    nearby_points
  GROUP BY
    fk_sgplaces ),
    -- 6 distance to centres
    scaled_distance as (
   
   select *,
       distance_to_centre_city / PERCENTILE_CONT(distance_to_centre_city,
        0.5) OVER(PARTITION BY name) AS distance_to_centre_city_scaled
   from 
   temp_distances_to_closer_city
   ),
    -- 7 join all
  join_all AS (
  SELECT
    a.* EXCEPT(olvin_category),
    IFNULL(olvin_category,
      '1') olvin_category,
    b.visits_from_corresponding_zipcode_population,
    c.population_visiting,
    c.population_visiting_equal_share,
    c.population_visiting_unique_device,
    d.visit_score_big_radius,
    d.count_poi_neighs_400,
    e.almanac_observed_visits_original,
    e.almanac_observed_visits_visit_share,
    e.almanac_observed_visits,
    f.distance_to_centre_city,
    f.distance_to_centre_city_scaled,
  FROM
    -- `storage-prod-olvin-com.smc_ground_truth_volume_model.model_input_1` a
    `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_1_table'] }}` a

  LEFT JOIN
    visits_from_corresponding_zip_pop b
  USING
    (fk_sgplaces,
      fk_zipcodes)
  LEFT JOIN
    population_visiting c
  USING
    (fk_sgplaces)
  LEFT JOIN
    get_visits_per_point_big_radius d
  USING
    (fk_sgplaces)
  LEFT JOIN
    get_visits_per_poi e
  USING
    (fk_sgplaces)
  LEFT JOIN
    scaled_distance f
  USING
    (fk_sgplaces) ),
  avg_by_category as (
select distinct ifnull(olvin_category, '1') olvin_category,
  PERCENTILE_DISC(visits_from_corresponding_zipcode_population, 0.5) OVER(partition by olvin_category) AS avg_visits_from_corresponding_zipcode_population,
  PERCENTILE_DISC(population_visiting, 0.5) OVER(partition by olvin_category) AS avg_population_visiting,
  PERCENTILE_DISC(population_visiting_equal_share, 0.5) OVER(partition by olvin_category) AS avg_population_visiting_equal_share,
  PERCENTILE_DISC(population_visiting_unique_device, 0.5) OVER(partition by olvin_category) AS avg_population_visiting_unique_device,
  PERCENTILE_DISC(visit_score_big_radius, 0.5) OVER(partition by olvin_category) AS avg_visit_score_big_radius,
  PERCENTILE_DISC(count_poi_neighs_400, 0.5) OVER(partition by olvin_category) AS avg_count_poi_neighs_400,
  PERCENTILE_DISC(almanac_observed_visits_original, 0.5) OVER(partition by olvin_category) AS avg_almanac_observed_visits_original,
  PERCENTILE_DISC(almanac_observed_visits_visit_share, 0.5) OVER(partition by olvin_category) AS avg_almanac_observed_visits_visit_share,
  PERCENTILE_DISC(almanac_observed_visits, 0.5) OVER(partition by olvin_category) AS avg_almanac_observed_visits,
  PERCENTILE_DISC(avg_distance_to_100_pois, 0.5) OVER (partition by olvin_category) AS avg_avg_distance_to_100_pois,
  PERCENTILE_DISC(avg_distance_to_10_pois, 0.5) OVER(partition by olvin_category) AS avg_avg_distance_to_10_pois,

FROM join_all
),
avg_for_zipcodes as (
SELECT 
  PERCENTILE_DISC(zipcode_population, 0.5) OVER() AS avg_zipcode_population,
  PERCENTILE_DISC(zipcode_area, 0.5) OVER() AS avg_zipcode_area,
  PERCENTILE_DISC(zipcode_density, 0.5) OVER() AS avg_zipcode_density,
FROM
  (select distinct fk_zipcodes,zipcode_population, zipcode_area, zipcode_density from join_all  )
LIMIT 1  
),
join_zipcodes as (
select * from avg_for_zipcodes, avg_by_category

),
fill_nas as (
select * EXCEPT(olvin_category,visits_from_corresponding_zipcode_population,population_visiting,population_visiting_equal_share,population_visiting_unique_device,visit_score_big_radius,count_poi_neighs_400,almanac_observed_visits_original,almanac_observed_visits_visit_share,almanac_observed_visits,avg_distance_to_100_pois, avg_distance_to_10_pois, zipcode_area, zipcode_density, zipcode_population,
avg_zipcode_population, avg_zipcode_area, avg_zipcode_density, avg_visits_from_corresponding_zipcode_population, avg_population_visiting, avg_population_visiting_equal_share, avg_population_visiting_unique_device, avg_visit_score_big_radius, avg_count_poi_neighs_400,avg_almanac_observed_visits_original,avg_almanac_observed_visits_visit_share, avg_almanac_observed_visits, avg_avg_distance_to_100_pois, avg_avg_distance_to_10_pois ), nullif(olvin_category, '1') olvin_category,
ifnull(visits_from_corresponding_zipcode_population,avg_visits_from_corresponding_zipcode_population) visits_from_corresponding_zipcode_population,
ifnull(population_visiting,avg_population_visiting) population_visiting,
ifnull(population_visiting_equal_share,avg_population_visiting_equal_share) population_visiting_equal_share,
ifnull(population_visiting_unique_device,avg_population_visiting_unique_device) population_visiting_unique_device,
ifnull(visit_score_big_radius,avg_visit_score_big_radius) visit_score_400_poi_ratio,
ifnull(count_poi_neighs_400,avg_count_poi_neighs_400) count_poi_neighs_400,
ifnull(almanac_observed_visits_original,avg_almanac_observed_visits_original) almanac_observed_visits_original,
ifnull(almanac_observed_visits_visit_share,avg_almanac_observed_visits_visit_share) almanac_observed_visits_visit_share,
ifnull(almanac_observed_visits,avg_almanac_observed_visits) almanac_observed_visits,
ifnull(avg_distance_to_100_pois,avg_avg_distance_to_100_pois) avg_distance_to_100_pois,
ifnull(avg_distance_to_10_pois,avg_avg_distance_to_10_pois) avg_distance_to_10_pois,
ifnull(zipcode_area, avg_zipcode_area) zipcode_area,
ifnull(zipcode_density, avg_zipcode_density) zipcode_density,
ifnull(zipcode_population, avg_zipcode_population) zipcode_population 

from join_all
left join join_zipcodes
using (olvin_category)
)
select * from fill_nas; 
END;
CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_2_table'] }}` as
select * from temp_fill_nas;
 