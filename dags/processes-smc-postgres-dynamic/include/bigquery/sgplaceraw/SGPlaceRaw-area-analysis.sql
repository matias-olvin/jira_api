CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
AS
SELECT a.* except(city,postal_code), city_id,
       CASE WHEN b.city IS NULL THEN a.city
            ELSE b.city END AS city,
       CASE WHEN b.postal_code IS NULL THEN a.postal_code
            ELSE b.postal_code END AS postal_code
FROM 
  `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` a
LEFT JOIN (
  SELECT a.pid, b.city_id, b.city, LPAD(CAST(c.pid AS STRING), 5, '0') AS postal_code
  FROM
    `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` a,
    `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['city_table'] }}` b,
    `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}` c
  WHERE ST_WITHIN(ST_GEOGPOINT(a.longitude, a.latitude), b.polygon)
    AND ST_WITHIN(ST_GEOGPOINT(a.longitude, a.latitude), ST_GEOGFROMTEXT(c.polygon))
) b
USING(pid)
