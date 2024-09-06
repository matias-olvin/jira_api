create or replace table
  `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}`

as

with
children_pois
as (
  SELECT pid as fk_child, fk_parents, standalone_bool, child_bool, parent_bool,
         ST_GEOGPOINT(longitude, latitude) as point_child
  FROM
    --`storage-prod-olvin-com.sg_places.places_dynamic`
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}` -- change to current sgplaces_dynamic
),

table_inner
as (
  select children_pois.fk_child, polygons_to_amend.id_amend as new_parent
  from
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['amend_polygons_table'] }}` polygons_to_amend
  inner join
    children_pois
  on ST_WITHIN(point_child, polygon_site)
),

fk_tenants_table
as(
  select children_pois.fk_child as pid,
         children_pois.point_child as point,
         (case   when (children_pois.fk_parents is null) and (table_inner.fk_child IS NOT NULL)
                then table_inner.new_parent
                else (case  when (children_pois.fk_parents = parents_to_amend.id_amend) and (table_inner.fk_child is null)
                            then null
                            else children_pois.fk_parents
                      end)
          end) as fk_parents_override,
         (case when children_pois.parent_bool then false
              else (case   when (children_pois.fk_parents is null) and (table_inner.fk_child IS NOT NULL) and not children_pois.parent_bool
                           then false
                           else (case  when (children_pois.fk_parents = parents_to_amend.id_amend) and (table_inner.fk_child is null)
                                       then true
                                       else children_pois.standalone_bool
                           end)
              end)
          end) as standalone_bool_override,
         (case when children_pois.parent_bool then false
              else (case   when (children_pois.fk_parents is null) and (table_inner.fk_child IS NOT NULL)
                           then true
                            else (case  when (children_pois.fk_parents = parents_to_amend.id_amend) and (table_inner.fk_child is null)
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
                `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['amend_polygons_table'] }}` polygons_to_amend
             ) as parents_to_amend
  on children_pois.fk_parents = parents_to_amend.id_amend
),

sg_places_tenants -- amend wrong polygons and add fk_parents_override / parenthood boolean_override columns
as(
  select sg_places.* EXCEPT(name, polygon_wkt, simplified_polygon_wkt, simplified_wkt_10_buffer),
         (case when id_amend is null then sg_places.name else polygons_to_amend.name end) as name,
         (case when id_amend is null then sg_places.polygon_wkt else polygons_to_amend.polygon_site end) as polygon_wkt,
         (case when id_amend is null then sg_places.simplified_polygon_wkt else polygons_to_amend.polygon_site end) as simplified_polygon_wkt,
         (case when id_amend is null then sg_places.simplified_wkt_10_buffer else ST_BUFFER(polygons_to_amend.polygon_site, 10) end) as simplified_wkt_10_buffer,
         fk_tenants_table.fk_parents_override,
         fk_tenants_table.standalone_bool_override,
         fk_tenants_table.child_bool_override,
         fk_tenants_table.parent_bool_override
  from
    --`storage-prod-olvin-com.sg_places.places_dynamic` as sg_places
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}` sg_places
  inner join
    fk_tenants_table
  on sg_places.pid = fk_tenants_table.pid
  left join
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['amend_polygons_table'] }}` polygons_to_amend
  on sg_places.pid = polygons_to_amend.id_amend
)



select *
from sg_places_tenants