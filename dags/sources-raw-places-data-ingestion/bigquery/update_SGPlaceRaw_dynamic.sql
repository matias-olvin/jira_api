CREATE OR REPLACE TABLE `{{ params['project'] }}.{{ params['postgres_data_dataset'] }}.{{ params['places_postgres_table'] }}` AS

with



---- Add unbranded malls that appear referenced as parents of any of the POIs in postgres table to that table
malls_table as
(
  SELECT pid
  FROM
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}`
  where naics_code=531120 and fk_sgbrands is null -- branded malls are already present in postgres table
),

parents_table as
(
  SELECT distinct fk_parents
  FROM
    `{{ params['project'] }}.{{ params['postgres_data_dataset'] }}.{{ params['places_postgres_table'] }}`
  where fk_sgbrands is not null
),

malls_to_append as -- append only those malls which appear in fk_parents_override column of postgres data
(
  select pid
  from malls_table
  inner join parents_table
  on parents_table.fk_parents = malls_table.pid
),

malls_data_from_sgplaces as
(
  select
    fk_sgbrands,
    malls_to_append.pid,
    name,
    industry,
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
    polygon_area_sq_ft,
    enclosed,
    phone_number,
    is_synthetic,
    includes_parking_lot,
    iso_country_code,
    fk_parents_override as fk_parents
  from
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}` sgplaces_dynamic
  inner join malls_to_append
  on malls_to_append.pid = sgplaces_dynamic.pid
),

postgres_with_malls as
(
select *
from
    `{{ params['project'] }}.{{ params['postgres_data_dataset'] }}.{{ params['places_postgres_table'] }}`
UNION ALL
select *
from malls_data_from_sgplaces
),


---- Look for children which share polygon with their respective parent to remove them from POSTGRES data (only those whose parent is a mall)
children_table as
(
  SELECT postgres.pid, --postgres.name, postgres.polygon_area_sq_ft,
         postgres.fk_parents,
         places_dynamic.polygon_wkt--,
         --postgres.top_category, postgres.sub_category, postgres.latitude, postgres.longitude
  FROM postgres_with_malls as postgres
  inner join
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}` places_dynamic
  on postgres.pid = places_dynamic.pid
  where postgres.fk_parents is not null
),

non_filtered as
(
  SELECT pid, naics_code, polygon_wkt --, polygon_area_sq_ft, name, top_category, sub_category, latitude, longitude
  FROM 
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}`
),

child_and_tenant as
(
  SELECT children_table.pid as pid,         --children_table.name as child_name, children_table.polygon_area_sq_ft as child_area,
         --children_table.fk_parents as tenant, non_filtered.name as tenant_name,  non_filtered.polygon_area_sq_ft as tenant_area,
         --children_table.top_category as child_top, children_table.sub_category as child_sub,
         children_table.polygon_wkt as child_polygon,
           --non_filtered.top_category as tenant_top,  non_filtered.sub_category as tenant_sub,
           non_filtered.polygon_wkt as tenant_polygon,
         --children_table.latitude as child_lat, children_table.longitude as child_long,
           --non_filtered.latitude as tenant_lat,  non_filtered.longitude as tenant_long,
           non_filtered.naics_code as tenant_naics
  FROM children_table
  left join non_filtered
  on non_filtered.pid=children_table.fk_parents
),

shared_polygons
as(
  select pid
  from child_and_tenant
  where st_astext(child_polygon) = st_astext(tenant_polygon)
        and child_and_tenant.tenant_naics=531120
)




select * except (remove_shared_polygon)
from(
  select postgres_with_malls.*,
         (case when shared_polygons.pid is not null
          then true
          else false
          end) as remove_shared_polygon
  from postgres_with_malls
  left join shared_polygons
  on shared_polygons.pid = postgres_with_malls.pid
)
where not remove_shared_polygon