-- Update categories
UPDATE 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
SET 
    naics_code = 452311, 
    top_category = 'General Merchandise Stores, including Warehouse Clubs and Supercenters',
    sub_category = 'Warehouse Clubs and Supercenters'
WHERE 
    fk_sgbrands IN (
        'SG_BRAND_98f6b474794d85f694762c9b52bbc351',-- Kmart
        'SG_BRAND_c4d65e0c51ea2fe6cef625aabba41ab6' --Big Lots
    )
;

-- Check that the NAICS / category is the same for the poi as for the corresponding brand
SELECT *
FROM(
SELECT pid, fk_sgbrands, name, naics_code, brand_naics
FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
LEFT JOIN(
  SELECT naics_code AS brand_naics, pid AS fk_sgbrands
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
) USING (fk_sgbrands)
WHERE brand_naics <> naics_code
)
WHERE IF(pid IS NULL, TRUE,
  ERROR(FORMAT("NAICS code of POI %s <> NAICS of the corresponding brand %s (%d <> %d) ",
               pid, fk_sgbrands, naics_code, brand_naics)))
;

-- Update open_hours
UPDATE 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table' ] }}` a
SET a.open_hours = b.actual_open_hours
FROM 
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['openings_adjustment_table'] }}` b
WHERE 
  a.pid = b.fk_sgplaces
  AND a.open_hours = b.sg_open_hours;


-- Update closing_date
UPDATE 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table' ] }}` a
SET a.closing_date = b.actual_closing_date
FROM 
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['openings_adjustment_table'] }}` b
WHERE 
  a.pid = b.fk_sgplaces;

-- Update opening_date
UPDATE 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table' ] }}` a
SET a.opening_date = b.actual_opening_date
FROM 
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['openings_adjustment_table'] }}` b
WHERE 
  a.pid = b.fk_sgplaces;
