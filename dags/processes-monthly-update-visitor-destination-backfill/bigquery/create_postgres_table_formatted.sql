CREATE OR REPLACE TABLE
 `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['visitor_brand_dest_table'] }}`
AS

WITH

num_of_devices AS(
  SELECT fk_sgplaces, any_value(total_devices) as total_devices
  FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}`
  GROUP BY fk_sgplaces
)


 SELECT fk_sgplaces as pid, brands,
        total_devices
 FROM(
   SELECT fk_sgplaces,
         REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(fk_sgbrands, shared_devices))), ',"shared_devices"', ''), '"fk_sgbrands":', '') as brands
   FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}`
   GROUP BY fk_sgplaces
 )
 INNER JOIN num_of_devices
   USING(fk_sgplaces)