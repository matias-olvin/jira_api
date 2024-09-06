CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
AS
WITH
base_table AS (
  SELECT
      a.pid,
      a.name,
      a.fk_parents,
      a.naics_code,
      a.top_category,
      a.sub_category,
      a.stock_symbol,
      a.stock_exchange,
      a.activity,
      b.url
  FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}` a
  LEFT JOIN
      `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['brand_urls_table'] }}` b
  ON
      a.pid = b.fk_sgbrands
  WHERE
      naics_code IN (
          SELECT
              naics_code
          FROM
              `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`
      )
  AND
      naics_code != 441120 AND naics_code != 441110 -- remove car dealers
)

SELECT base_table.*,
       IFNULL(almanac_category, "Other") AS tenant_type,
       IFNULL(luxury,0) AS luxury
FROM
      base_table
LEFT JOIN
      `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
  USING(naics_code)
LEFT JOIN(
    SELECT fk_sgbrand AS pid, 1 AS luxury
    FROM
      `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['luxury_brands_table'] }}`
    )
  USING(pid)
