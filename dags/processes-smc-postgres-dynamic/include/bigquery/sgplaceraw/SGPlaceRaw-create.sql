CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
AS
with
-- base postgres table. 
postgres_base as (
  SELECT
    pid,
    name,
    fk_sgbrands,
    industry,
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
    polygon_area_sq_ft,
    enclosed,
    phone_number,
    is_synthetic,
    includes_parking_lot,
    iso_country_code,
    fk_parents_override as fk_parents,
    opening_date,
    closing_date,
    CASE 
      WHEN closing_date = '2100-01-01' THEN 1
      WHEN closing_date IS NOT NULL THEN
        CASE 
          WHEN DATE_DIFF(DATE_SUB(DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 1 DAY), INTERVAL 1 MONTH), closing_date, MONTH) >= 6 THEN 0
          ELSE 2
        END 
      ELSE 1
    END as opening_status
  FROM 
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  WHERE 
      sensitive IS FALSE AND
      postal_code NOT LIKE '969__%' AND -- remove Guam places
      region != 'PR' AND -- remove Puerto Rico places
      (naics_code != 441120 AND naics_code != 441110) AND -- remove car dealers
      fk_sgbrands IS NOT NULL  -- remove unbranded
),

-- Add unbranded malls that appear referenced as parents of any of the POIs in postgres table to that table
malls_table as (
  SELECT pid
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  where
    naics_code=531120 and 
    fk_sgbrands is null -- branded malls are already present in postgres table
),

parents_table as
(
  SELECT distinct fk_parents
  FROM
    postgres_base
  where fk_sgbrands is not null
),

malls_to_append as ( -- append only those malls which appear in fk_parents_override column of postgres data
  select pid
  from malls_table
  inner join parents_table
  on parents_table.fk_parents = malls_table.pid
),

malls_data_from_sgplaces as (
  select
    malls_to_append.pid,
    name,
    fk_sgbrands,
    industry,
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
    polygon_area_sq_ft,
    enclosed,
    phone_number,
    is_synthetic,
    includes_parking_lot,
    iso_country_code,
    fk_parents_override as fk_parents,
    opening_date,
    closing_date,
    CASE 
      WHEN closing_date is not null
        THEN
          CASE 
              WHEN 
                DATE_DIFF(DATE_SUB(DATE_ADD(LAST_DAY(CURRENT_DATE()), INTERVAL 1 DAY), INTERVAL 1 MONTH), closing_date, MONTH) >= 6 
                  THEN 0
              ELSE 2
            END 
      ELSE 1
    END as opening_status
  from
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` sgplaces_dynamic
  inner join malls_to_append
  on malls_to_append.pid = sgplaces_dynamic.pid
),

postgres_malls as (
  select *
  from
      postgres_base
  UNION ALL
  select *
  from malls_data_from_sgplaces
),

big_boxes AS (
    SELECT 
        name, 
        pid, 
        polygon_wkt
    FROM (
        SELECT 
            name, 
            pid
        FROM
            postgres_malls
        WHERE polygon_area_sq_ft > 50000 AND naics_code!=531120
    )
    LEFT JOIN (
        SELECT 
            pid, 
            polygon_wkt
        FROM
            `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
    )
    USING(pid)
),

big_box_shared_polygons AS (
    SELECT *
    FROM (
        SELECT  
            a.name AS name1,
            b.name AS name2,
            a.pid AS pid1,
            b.pid AS pid2,
            a.polygon_wkt AS polygon_wkt1,
            b.polygon_wkt AS polygon_wkt2
        FROM big_boxes a
        LEFT OUTER JOIN big_boxes b
        ON a.pid!= b.pid
    )
    WHERE st_astext(polygon_wkt1) = st_astext(polygon_wkt2)
),

pid_wrong_area AS (
    SELECT DISTINCT pid1
    FROM big_box_shared_polygons
)

SELECT 
    * EXCEPT(pid1, polygon_area_sq_ft),
    (
        CASE 
            WHEN pid1 is null THEN polygon_area_sq_ft
            ELSE null
        END
    ) AS polygon_area_sq_ft,
    NULL AS fk_sgcenters
FROM
    postgres_malls
LEFT JOIN pid_wrong_area
ON pid = pid1
