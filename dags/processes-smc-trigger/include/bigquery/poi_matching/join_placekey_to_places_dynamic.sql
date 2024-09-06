create or replace table
-- `storage-prod-olvin-com.accessible_by_sns.places_dynamic_placekey`
`{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_placekey_table'] }}`
AS
  SELECT
    pid
    , pid AS fk_sgplaces
    , name
    , brands
    , ST_GEOGPOINT(longitude, latitude) AS geo_point
    , street_address
    , city
    , region
    , postal_code
    , simplified_polygon_wkt
    , sg_id AS placekey
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  WHERE CONTAINS_SUBSTR(sg_id, '@')


