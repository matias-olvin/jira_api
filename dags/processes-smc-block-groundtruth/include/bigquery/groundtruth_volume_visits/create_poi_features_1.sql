-- create or replace table `olvin-sandbox-20210203-0azu4n.ella_test.all_pois_stage_1` as
BEGIN
CREATE temp TABLE temp_list_pois AS
  SELECT
      pid AS fk_sgplaces,
--      ST_ASTEXT(ST_GEOGPOINT(longitude, latitude)) AS long_lat_pl,
      long_lat_point AS long_lat_pl,
      fk_sgbrands,
      postal_code,
      name,
      naics_code,
      region,
      ST_AREA( (polygon_wkt)) AS polygon_area,
       IF
      (fk_parents IS NULL, 
        1,
        0) AS standalone_bool

  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
    WHERE iso_country_code = 'US'
      AND pid IN(
        SELECT pid
        FROM
          `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
        )
  ;
END;

BEGIN
CREATE temp TABLE temp_clustered_filtered_places 
CLUSTER BY long_lat_point
AS
SELECT *
FROM
        `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
WHERE iso_country_code = 'US'
  AND pid IN(
    SELECT pid
    FROM
      `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    )
  ;
END;

BEGIN
CREATE temp TABLE temp_joining_pois_staging as 
  SELECT
  a.pid AS fk_sgplaces,
  b.naics_code nearby_naics_code,
  a.naics_code,
  b.pid AS nearby_poi,
  a.*EXCEPT(pid,
    naics_code,long_lat_point,polygon_wkt,simplified_polygon_wkt,simplified_wkt_10_buffer),
  ST_DISTANCE(a.long_lat_point, b.long_lat_point) distance
  FROM
    temp_clustered_filtered_places a,
    temp_clustered_filtered_places b
  WHERE
  ST_DWITHIN( a.long_lat_point, b.long_lat_point, 1500);
END;

BEGIN
CREATE temp TABLE temp_joining_pois as 
  SELECT
  fk_sgplaces,
  nearby_naics_code,
  naics_code,
  nearby_poi,
  distance
  FROM
    temp_joining_pois_staging;
END;

BEGIN
CREATE temp TABLE temp_distance_to_nearest_pois as 
SELECT
        fk_sgplaces,  nearby_poi, distance, rank, naics_code, nearby_naics_code
    FROM (
      SELECT
        *, 
        RANK() OVER (PARTITION BY fk_sgplaces ORDER BY distance) rank
      FROM temp_joining_pois
    WHERE distance != 0);
END;
BEGIN
CREATE temp TABLE temp_drop_duplicates as 
with join_category_median_visits as (
  select a.*, b.olvin_category, c.olvin_category_median_visits from temp_distance_to_nearest_pois a
  left join  `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}` b using (naics_code)
  left join  `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['median_ground_truth_category_visits'] }}` c on b.olvin_category = c.olvin_category
  
  ),

  poi_count_less_400 as (
  select distinct fk_sgplaces, COUNT(fk_sgplaces) count_pois_less_than_400,
  SUM(olvin_category_median_visits) sum_nearest_poi_400_median_category_visits,
  from join_category_median_visits
  where distance <= 400
  group by fk_sgplaces
  ),

  avg_distance_to_10_nearest_pois AS (
  SELECT
  distinct fk_sgplaces,
    AVG(distance) OVER (partition by fk_sgplaces ) avg_distance_to_10_pois 
  FROM temp_distance_to_nearest_pois
  where rank <=10
),
  avg_distance_to_100_nearest_pois AS (
  SELECT
   distinct fk_sgplaces,
    AVG(distance) OVER (partition by fk_sgplaces ) avg_distance_to_100_pois 
  FROM temp_distance_to_nearest_pois
  where rank <=100
),
  distance_to_nearest_store AS (
  SELECT
   distinct fk_sgplaces,
    AVG(distance) OVER (partition by fk_sgplaces ) min_distance_to_next_store 
  FROM temp_distance_to_nearest_pois
  where rank <=1
),
 count_parking_200m_ as (
    SELECT fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_parking_200m
    from temp_distance_to_nearest_pois
    where distance < 200 and naics_code = 812930
 ),
  count_parking_1000m_ as (
    SELECT distinct fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_parking_1km
    from temp_distance_to_nearest_pois
    where distance < 1000 and nearby_naics_code = 812930
 ),
 count_transport_200m_ as (
    SELECT distinct fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_transport_200m
    from temp_distance_to_nearest_pois
    where distance < 200 and nearby_naics_code in (select naics_code FROM 
    `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['group_category_naics_code'] }}`
    -- `storage-prod-olvin-com.ground_truth_volume_model.group_category_naics_code`
    WHERE group_category_name = 'transport' )
 ),
  count_transport_1000m_ as (
    SELECT distinct fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_transport_1km
    from temp_distance_to_nearest_pois
    where distance < 1000 and nearby_naics_code in (select naics_code FROM 
    `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['group_category_naics_code'] }}`
    -- `storage-prod-olvin-com.ground_truth_volume_model.group_category_naics_code`
    WHERE group_category_name = 'transport' )
 ),
   count_education_1000m_ as (
    SELECT distinct fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_education_1km
    from temp_distance_to_nearest_pois
    where distance < 1000 and nearby_naics_code in (select naics_code FROM 
    `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['group_category_naics_code'] }}`
    -- `storage-prod-olvin-com.ground_truth_volume_model.group_category_naics_code`
    WHERE group_category_name = 'education' )
 ),
   count_education_200m_ as (
    SELECT distinct fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_education_200m
    from temp_distance_to_nearest_pois
    where distance < 200 and nearby_naics_code in (select naics_code FROM 
    `{{ var.value.env_project }}.{{ params['ground_truth_volume_dataset'] }}.{{ params['group_category_naics_code'] }}`
    -- `storage-prod-olvin-com.ground_truth_volume_model.group_category_naics_code`
    WHERE group_category_name = 'education' )
 ),
 count_200m_competitors_ as (
   SELECT distinct fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_200m_competitors
    from temp_distance_to_nearest_pois
    where distance < 200 and naics_code = nearby_naics_code and rank >1 
 
 ),
  count_1000m_competitors_ as (
   SELECT distinct fk_sgplaces,
    COUNT(*) OVER (partition by fk_sgplaces) AS count_1km_competitors
    from temp_distance_to_nearest_pois
    where distance < 200 and naics_code = nearby_naics_code and rank >1 
 
 ),

join_all_distances as (
select t1.fk_sgplaces, t1.fk_sgbrands, t1.postal_code, t1.name, t1.naics_code, t1.region, t1.polygon_area, t1.long_lat_pl, t1.standalone_bool,  avg_distance_to_100_pois,avg_distance_to_10_pois 
from  temp_list_pois t1
-- join_nearest_poi_400_median_category_visits t1
left join avg_distance_to_10_nearest_pois t2 using (fk_sgplaces)
left join avg_distance_to_100_nearest_pois t3 using (fk_sgplaces)

)

select t2.*, 
IFNULL(t1.min_distance_to_next_store,0) min_distance_to_next_store,
IFNULL(count_pois_less_than_400,0) count_pois_less_than_400, 
IFNULL(sum_nearest_poi_400_median_category_visits,0) sum_nearest_poi_400_median_category_visits, 
IFNULL(count_education_200m,0) count_education_200m, 
IFNULL(count_education_1km,0) count_education_1km ,
IFNULL(count_transport_200m,0) count_transport_200m , 
IFNULL(count_transport_1km,0) count_transport_1km, 
IFNULL(count_parking_200m,0) count_parking_200m ,
IFNULL(count_parking_1km,0) count_parking_1km from join_all_distances t2
left join distance_to_nearest_store t1 using (fk_sgplaces)
left join poi_count_less_400 using (fk_sgplaces)
left join count_parking_200m_ using (fk_sgplaces)
left join count_parking_1000m_ using (fk_sgplaces)
left join count_transport_200m_ using (fk_sgplaces)
left join count_transport_1000m_ using (fk_sgplaces)
left join count_education_200m_ using (fk_sgplaces)
left join count_education_1000m_ using (fk_sgplaces)
left join count_200m_competitors_ using (fk_sgplaces)
left join count_1000m_competitors_ using (fk_sgplaces);
END;
BEGIN
CREATE temp TABLE last_features_1 as 
 with
  -- ALFONSO QUERIES    
  joining_zipcodes AS (
  SELECT
    temp_drop_duplicates.*,
    z.pid AS fk_zipcodes,
    z.population AS zipcode_population,
    ST_AREA(ST_GEOGFROMTEXT(z.polygon)) AS zipcode_area,
    z.population / ST_AREA(ST_GEOGFROMTEXT(z.polygon)) AS zipcode_density
  FROM
    temp_drop_duplicates
  LEFT JOIN (
    SELECT
      *
    FROM

      `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}`)z
      -- `storage-prod-olvin-com.area_geometries.zipcodes`) z
  ON
    LPAD(CAST(pid AS STRING ),
      5,
      '0' ) = postal_code ),
      
  -- OLVIN CATEGORY
  joining_olvin_category AS (
  SELECT
    *
  FROM
    joining_zipcodes
  LEFT JOIN (
    SELECT
      naics_code,
      olvin_category
    FROM
     `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`)
      -- `storage-prod-olvin-com.sg_base_tables.all_naics_code_with_olvin_category` )
  USING
    (naics_code) ),

drop_duplicates  AS
(
SELECT * EXCEPT(row_number)
FROM (
  SELECT
      *,
      ROW_NUMBER()
          OVER (PARTITION BY fk_sgplaces)
          as row_number
  FROM joining_olvin_category
)
WHERE row_number = 1
)
select * from drop_duplicates;
END;
CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['create_poi_features_1_table'] }}` as
SELECT
  *
FROM last_features_1;