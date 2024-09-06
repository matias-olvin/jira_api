CREATE OR REPLACE TABLE
 `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['visitor_brand_dest_percentages_table'] }}`
AS

-- Creates visitor brand destinations with percentages (instead of device counts) and obfuscates the 
-- values using the first appearing digit of the fk_sgbrand identifier. 

WITH

observed_connections AS (
  SELECT * FROM `{{ params['storage-prod'] }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}`
  LEFT JOIN `{{ params['storage-prod'] }}.{{ params['postgres_dataset'] }}.{{ params['brands_table'] }}`
  ON fk_sgbrands=pid
  WHERE  SUBSTR(CAST(naics_code AS STRING), 1, 2) NOT IN ('11', '21', '22', '23', '33', '48', '49', '54', '55', '56', '61', '62', '92')
  -- Exclude four-digit NAICS code prefixes
  AND SUBSTR(CAST(naics_code AS STRING), 1, 4) NOT IN ('5311', '5312') 

)

, num_of_devices AS(
  SELECT fk_sgplaces, any_value(total_devices) as total_devices
  FROM observed_connections
  GROUP BY fk_sgplaces
)


 SELECT fk_sgplaces as pid, brands,
        total_devices
 FROM(
   SELECT fk_sgplaces,
         REPLACE(
          REPLACE(
            TO_JSON_STRING(
              ARRAY_AGG(
                STRUCT(fk_sgbrands,
                       CAST(ROUND(100*(shared_devices/total_devices)*(1+CAST(REGEXP_EXTRACT(fk_sgbrands, r'(\d)') AS INT64)/100), 2) AS STRING)  AS shared_devices
                      )
                       )
                          )
          
      , ',"shared_devices"', '')
      , '"fk_sgbrands":', '') 
                              AS brands
   FROM observed_connections
   GROUP BY fk_sgplaces
 )
 INNER JOIN num_of_devices
   USING(fk_sgplaces)