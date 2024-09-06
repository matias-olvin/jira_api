DECLARE max_last_seen DEFAULT (
    SELECT
        MAX(last_seen)
    FROM 
        `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_history_table'] }}`
);

CREATE OR REPLACE TABLE 
  `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
-- CLUSTER BY pid  -- uncomment if table is clustered
AS 

WITH active_places AS (
    SELECT
        olvin_id as pid,
        fk_sgbrands,
        name,
        brands,
        top_category,
        sub_category,
        naics_code,
        site_id,
        latitude,
        longitude,
        street_address,
        city,
        region,
        postal_code,
        open_hours,
        category_tags,
        polygon_wkt,
        polygon_class,
        enclosed,
        iso_country_code,
        region_id,
        long_lat_point,
        simplified_polygon_wkt,
        simplified_wkt_10_buffer,
        fk_parents_olvin as fk_parents,
        standalone_bool,
        child_bool,
        parent_bool,
        polygon_area_sq_ft,
        industry,
        timezone,
        phone_number,
        is_synthetic,
        includes_parking_lot,
        case
            when length(opened_on) = 10
                then date(opened_on) 
            when length(opened_on) = 7
                then date(concat(opened_on, "-01"))
        end as opening_date,  -- Logic for opening/closing dates
        closed_on as closing_date
    FROM
        `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_history_table'] }}`
    WHERE
        active
        AND last_seen > DATE_SUB(max_last_seen, INTERVAL 3 MONTH)
),

inactive_brands AS( -- Check for possible parenthoods of inactive brands to update them
  SELECT *
  FROM(
    SELECT pid AS inactive_fk_sgbrands, fk_parents AS amended_fk_sgbrands
    FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_history_table'] }}`
    WHERE activity='inactive'
  )
  INNER JOIN(
    SELECT pid AS amended_fk_sgbrands, name AS amended_brand
    FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
  )
  USING(amended_fk_sgbrands)
),

active_places_brands AS(
  SELECT *
  FROM(
    SELECT
      pid,
      IFNULL(amended_fk_sgbrands, fk_sgbrands) AS fk_sgbrands,
      name,
      IFNULL(amended_brand, brands) AS brands,
      top_category,
      sub_category,
      naics_code,
      site_id,
      latitude,
      longitude,
      street_address,
      city,
      region,
      postal_code,
      open_hours,
      category_tags,
      polygon_wkt,
      polygon_class,
      enclosed,
      iso_country_code,
      region_id,
      long_lat_point,
      simplified_polygon_wkt,
      simplified_wkt_10_buffer,
      fk_parents,
      standalone_bool,
      child_bool,
      parent_bool,
      polygon_area_sq_ft,
      industry,
      timezone,
      phone_number,
      is_synthetic,
      includes_parking_lot,
      opening_date,
      closing_date
    FROM active_places
    LEFT JOIN inactive_brands -- Update inactive fk_sgbrand based on parenthood relationship
      ON fk_sgbrands = inactive_fk_sgbrands
  )
  WHERE fk_sgbrands IN(
    SELECT pid
    FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
  ) -- Remove pois whose brand is not in brands_dynamic table
  OR fk_sgbrands IS NULL
),

children_pois as (
  SELECT 
    pid as fk_child, 
    fk_parents, 
    standalone_bool, 
    child_bool, 
    parent_bool,
    ST_GEOGPOINT(longitude, latitude) as point_child
  FROM active_places_brands
),

table_inner as (
  select 
    children_pois.fk_child, 
    polygons_to_amend.id_amend as new_parent
  from
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['polygons_to_amend_table'] }}` polygons_to_amend
  inner join children_pois
  on ST_WITHIN(point_child, polygon_site)
),

fk_tenants_table as(
  select 
    children_pois.fk_child as pid,
    children_pois.point_child as point,
    (case 
        when (children_pois.fk_parents is null) and (table_inner.fk_child IS NOT NULL)
            then table_inner.new_parent
        else (case  
            when (children_pois.fk_parents = parents_to_amend.id_amend) and (table_inner.fk_child is null)
                then null
            else children_pois.fk_parents
        end)
    end) as fk_parents_override,
    (case 
        when children_pois.parent_bool 
            then false
        else (case   
            when (children_pois.fk_parents is null) and (table_inner.fk_child IS NOT NULL) and not children_pois.parent_bool
                then false
            else (case
                when (children_pois.fk_parents = parents_to_amend.id_amend) and (table_inner.fk_child is null)
                    then true
                else children_pois.standalone_bool
            end)
        end)
    end) as standalone_bool_override,
    (case 
        when children_pois.parent_bool 
            then false
        else (case   
            when (children_pois.fk_parents is null) and (table_inner.fk_child IS NOT NULL)
                then true
            else (case  
                when (children_pois.fk_parents = parents_to_amend.id_amend) and (table_inner.fk_child is null)
                    then false
                else children_pois.child_bool
            end)
        end)
    end) as child_bool_override,
    children_pois.parent_bool as parent_bool_override
    -- Same logic is reproduced in all previous case/else statements
        -- parent_bool has priority (a grandpa-parent-child) will have 2 POIs w/ parent_bool and 1 w/ child_bool
        -- so boolean logic only applies where parent_bool = false (in any case parent_bool_override = parent_bool)
            -- If POI has no parents but IT'S INSIDE AN AMENDED POLYGON
                -- Set fk_parents_overide = amend POI id  ; child_bool_override = true
            -- If POI has as a parent an amended POI and it is physically OUTSIDE THE AMENDED POLYGON
                -- Set fk_parents = null. ; standalone_bool_override = true
            -- If POI is not inside an amended polygon and its parent (if has one) has not been amended
                -- Copy the previous info to {}_override columns
    from children_pois
    LEFT JOIN
        table_inner
    USING(fk_child)
    left join (
        select distinct id_amend
        from 
            `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['polygons_to_amend_table'] }}` polygons_to_amend
    ) as parents_to_amend
    on children_pois.fk_parents = parents_to_amend.id_amend
),

sg_places_tenants as( -- amend wrong polygons and add fk_parents_override / parenthood boolean_override columns
  select 
    sg_places.* EXCEPT(name, polygon_wkt, simplified_polygon_wkt, simplified_wkt_10_buffer),
    (case when id_amend is null then sg_places.name else polygons_to_amend.name end) as name,
    (case when id_amend is null then sg_places.polygon_wkt else polygons_to_amend.polygon_site end) as polygon_wkt,
    (case when id_amend is null then sg_places.simplified_polygon_wkt else polygons_to_amend.polygon_site end) as simplified_polygon_wkt,
    (case when id_amend is null then sg_places.simplified_wkt_10_buffer else ST_BUFFER(polygons_to_amend.polygon_site, 10) end) as simplified_wkt_10_buffer,
    fk_tenants_table.fk_parents_override,
    fk_tenants_table.standalone_bool_override,
    fk_tenants_table.child_bool_override,
    fk_tenants_table.parent_bool_override
  from
    active_places sg_places  
  inner join
    fk_tenants_table
  on sg_places.pid = fk_tenants_table.pid
  left join
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['polygons_to_amend_table'] }}` polygons_to_amend
  on sg_places.pid = polygons_to_amend.id_amend
),

brand_hours_raw as (
  SELECT 
    pid, 
    fk_sgbrands as brand, 
    open_hours
  FROM sg_places_tenants
  where fk_sgbrands is not null
),

diff_openhours as (
  select distinct 
    brand, 
    open_hours, 
    count(pid) as OH_num
  from brand_hours_raw
  where open_hours is not null
  group by brand, open_hours
  order by brand
),

diff_brand as (
  select distinct 
    brand, 
    count(pid) as brand_num
  from brand_hours_raw
  group by brand
  order by brand
),

diff_brand_notnull as (
  select distinct 
    brand, 
    count(pid) as brand_not_null_num
  from brand_hours_raw
  where open_hours is not null
  group by brand
  order by brand
),

merged_table as (
  select distinct 
    diff_openhours.brand, 
    open_hours, 
    OH_num, 
    brand_num,
    brand_num-brand_not_null_num as null_num,
    100*OH_num/brand_not_null_num as percent_OH,
    100*(1-brand_not_null_num/brand_num) as percent_null
  from diff_openhours
  left join diff_brand
  on diff_openhours.brand = diff_brand.brand
  left join diff_brand_notnull
  on diff_openhours.brand = diff_brand_notnull.brand
  order by brand, percent_OH desc
),

max_OH_per_brand as (
  select distinct brand, max(percent_OH) as max_percent_OH
  from merged_table
  group by brand
  order by brand
),

MostCommon_OpenHour_perBrand as (
    select 
        brand, 
        open_hours as OpeningHours,
        OH_num as stores_sharing_OpeningHours, 
        brand_num as total_stores_of_brand,
        null_num as total_null, 
        percent_null, 
        percent_OH
    from (
        select 
            merged_table.*, 
            max_OH_per_brand.max_percent_OH, 
            row_number() over (partition by merged_table.brand) row_num
        from max_OH_per_brand
        inner join merged_table
        on 
            max_OH_per_brand.brand = merged_table.brand and
            max_OH_per_brand.max_percent_OH = merged_table.percent_OH
    )
    where row_num = 1
),

hours_to_correct as (-- table with open_hours to assign if we find a POI of the brand that has no info in that field
    select 
        brand,
        OpeningHours as open_hours_brand
    from MostCommon_OpenHour_perBrand
    where 
        stores_sharing_OpeningHours >= 5 and (
            total_null < 100*stores_sharing_OpeningHours or 
            (total_null < 2000*stores_sharing_OpeningHours and percent_OH>70) 
        )
    order by total_null desc
)
-- Conditions: most common OH appears at least 5 times
--      If this represents > 70% of known opening info (we are more confident about it) apply it to nulls if they are less than 2k times known
--      If % is lower (less confidence) set extrapolation threshold to 100 times

SELECT 
    sgplaces.* except(open_hours),
    (case 
        when open_hours is not null
            then open_hours
        else open_hours_brand
    end) as open_hours
FROM sg_places_tenants sgplaces
left join hours_to_correct
on sgplaces.fk_sgbrands = hours_to_correct.brand
