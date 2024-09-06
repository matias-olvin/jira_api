CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['malls_base_table'] }}`
AS
  SELECT pid, name, naics_code, latitude, longitude, street_address, city_id, city, region, postal_code, category_tags, polygon_wkt, iso_country_code, polygon_area_sq_ft, opening_date, closing_date, opening_status, ST_GEOGPOINT(longitude, latitude) AS point
  FROM
    `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` a
--    `storage-prod-olvin-com.smc_sg_places.SGPlaceRaw` a
  INNER JOIN(
    SELECT pid, polygon_wkt
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
--      `storage-prod-olvin-com.smc_sg_places.places_dynamic`
  )
  USING(pid)
  WHERE a.naics_code = 531120
