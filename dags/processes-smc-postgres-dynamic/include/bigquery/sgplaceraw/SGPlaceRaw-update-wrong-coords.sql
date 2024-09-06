CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['wrong_coords_blacklist_table'] }}`
--  `storage-prod-olvin-com.sg_places.wrong_coords_blacklist`
AS

SELECT a.*
FROM
  `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['wrong_coords_blacklist_table'] }}` a
--  `storage-prod-olvin-com.sg_places.wrong_coords_blacklist` a
INNER JOIN
  `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
--  `storage-prod-olvin-com.smc_sg_places.SGPlaceRaw`
USING(pid)
WHERE ST_DISTANCE(ST_GEOGPOINT(longitude, latitude),
                  ST_GEOGPOINT(wrong_longitude, wrong_latitude)) < 50
