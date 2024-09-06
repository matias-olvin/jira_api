BEGIN
  CREATE TEMP TABLE temp_get_region_id AS (
  SELECT 
      *, 
      ROW_NUMBER() OVER () region_id 
    FROM (
      SELECT 
        distinct region 
      FROM  
        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw`
      )
  );
END
;
BEGIN
  CREATE TEMP TABLE temp_clean_data AS (
    SELECT 
      placekey as sg_id, 
      safegraph_brand_ids as fk_sgbrands, 
      parent_placekey as fk_parents,
      location_name as name, 
      brands, 
      top_category, 
      sub_category, 
      naics_code, 
      latitude, 
      longitude, 
      street_address, 
      city, 
      region, 
      postal_code, 
      open_hours, 
      category_tags, 
      ST_GEOGFROMTEXT(polygon_wkt, make_valid=>TRUE) polygon_wkt, 
      polygon_class, 
      enclosed, 
      iso_country_code,
      ST_SIMPLIFY(ST_GEOGFROMTEXT(polygon_wkt, make_valid=>TRUE) , 10) as simplified_polygon_wkt,
      opened_on,
      closed_on,
      cast(phone_number as string) as phone_number,
      is_synthetic,
      includes_parking_lot
    FROM 
      `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw` 
    where
      iso_country_code != 'GB'
  );
END
;
BEGIN
  CREATE OR REPLACE TEMP TABLE temp_join_region_id as (
    SELECT 
      clean_data.* EXCEPT(simplified_polygon_wkt), 
      region_id, 
      ST_GEOGPOINT(longitude, latitude) as long_lat_point,
      CASE 
        WHEN ST_AREA(simplified_polygon_wkt) < 10  THEN polygon_wkt
      ELSE simplified_polygon_wkt
      end as 
        simplified_polygon_wkt
    FROM 
      temp_clean_data clean_data
    left join
      temp_get_region_id get_region_id
      using (region)
  );
END
;
BEGIN 
  create temp table temp_area_calculations AS (
    select 
      *,
      avg(area_) over (partition by naics_code) AS mean_area,
      stddev(area_) over (partition by naics_code) as std_area
    from (
      select
        sg_id,
        name,
        naics_code,
        sub_category,
        ST_AREA(simplified_polygon_wkt) as area_
      from  
        temp_clean_data
    )
  );
END
;
BEGIN
  CREATE TEMP TABLE temp_get_large_area_placekeys AS (
  SELECT * 
  FROM temp_area_calculations
  WHERE area_ > (mean_area + std_area*10) and area_ > 25000
  -- ORDER BY area_
  );
END
;
BEGIN
  CREATE TEMP TABLE temp_remove_large_area_placekeys AS
    SELECT * 
    from 
      temp_join_region_id 
    where 
      sg_id not in (
        select sg_id 
        from temp_get_large_area_placekeys
      );
END
;
BEGIN
  CREATE TEMP TABLE temp_add_polygon_area_sq_ft AS 
WITH 
  add_10_buffer as (
    select 
      *, 
      ST_BUFFER(simplified_polygon_wkt, 10) simplified_wkt_10_buffer 
    from 
      temp_remove_large_area_placekeys 
  ),
  parents AS (
    SELECT
      a.*,
      b.fk_parents AS fk_parents2
    FROM
      add_10_buffer a
    LEFT JOIN (
      SELECT
        sg_id,
        fk_parents,
        top_category
      FROM
        add_10_buffer ) b
    ON
      a.sg_id = b.fk_parents 
  ),
  create_bool_columns AS (
    SELECT
      *,
      CASE
        WHEN fk_parents IS NULL AND fk_parents2 IS NULL THEN TRUE
      ELSE
      FALSE
    END
      AS standalone_bool,
      CASE
        WHEN fk_parents IS NOT NULL AND fk_parents2 IS NULL THEN TRUE
      ELSE
      FALSE
    END
      AS child_bool,
      CASE
        WHEN fk_parents IS NULL AND fk_parents2 IS NOT NULL THEN TRUE
        WHEN fk_parents IS NOT NULL
      AND fk_parents2 IS NOT NULL THEN TRUE
      ELSE
      FALSE
    END
      AS parent_bool
    FROM
      parents 
  ),
  add_two_digit_naics as (
    SELECT  
      a.*, 
      b.two_digit_code as naics_industry, 
      b.two_digit_title as industry
  from 
    create_bool_columns a
  left join (
    SELECT DISTINCT 
      two_digit_code, 
      two_digit_title 
    FROM 
      `{{ params['project'] }}.{{ params['base_tables_data_dataset'] }}.{{ params['different_digit_naics_code_table'] }}`
  )  b
  on 
    CAST(LEFT(CAST(a.naics_code AS STRING), 2) as int64) = CAST(b.two_digit_code as INT64)
  ),
  add_polygon_area_sq_ft as (
    SELECT 
      *, 
      ST_AREA(polygon_wkt) * 10.764 as polygon_area_sq_ft 
    from 
      add_two_digit_naics
  )
  SELECT * FROM add_polygon_area_sq_ft;
END
;
BEGIN
  create temp table temp_detect_duplicates AS (
    SELECT
      * EXCEPT(fk_parents2),
      ROW_NUMBER() OVER (PARTITION BY sg_id) AS row_num
    FROM
      temp_add_polygon_area_sq_ft
  );
END
;
BEGIN
  create temp table temp_drop_duplicates AS (
    SELECT
      * EXCEPT(row_num)
    FROM temp_detect_duplicates
    WHERE
      row_num = 1 
  );
END
;
BEGIN
  CREATE TEMP TABLE temp_remove_null_parents AS
WITH
  set_timezone AS (
    SELECT
      places.*,
      tt.tzid as timezone
    FROM
      temp_drop_duplicates places
    left join
      `{{ params['project'] }}.{{ params['geometry_dataset'] }}.{{ params['timezones_table'] }}`  tt
    on
      ST_WITHIN(places.long_lat_point, tt.geometry)
  ),
  remove_null_parents AS (
    SELECT
      a.* EXCEPT(fk_parents),
      CASE WHEN b.sg_id IS NULL
          THEN NULL
          ELSE a.fk_parents
          END AS fk_parents
    FROM
      set_timezone a
    left join
      set_timezone b
    on
      a.fk_parents = b.sg_id
  )
  SELECT
    *
  FROM
    remove_null_parents;
END
;
BEGIN
  CREATE TEMP TABLE update_naics_brand_based AS (
  SELECT place.* EXCEPT(naics_code, top_category, sub_category),
       IFNULL(brand.naics_code, place.naics_code) AS naics_code,
       IFNULL(brand.top_category, place.top_category) AS top_category,
       IFNULL(brand.sub_category, place.sub_category) AS sub_category
  FROM
      temp_remove_null_parents  place
  LEFT JOIN
      `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_brands` brand
  ON brand.pid = fk_sgbrands
  );
END
;

CREATE or REPLACE TABLE `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places` 
partition by range_bucket(region_id, GENERATE_ARRAY(0,200,1)) 
cluster by long_lat_point, sg_id, naics_code, fk_sgbrands
AS 
SELECT * FROM update_naics_brand_based