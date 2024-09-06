SELECT
    fk_sgbrands,
    pid,
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
FROM 
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_places_table'] }}`
WHERE 
    naics_code IN (
        SELECT 
            naics_code 
        FROM
            `{{ params['project'] }}.{{ params['base_tables_data_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}` 
    )  -- remove sensitive NAICS
    AND
        postal_code NOT LIKE '969__%'  -- remove Guam places
    AND
        naics_code != 441120 AND naics_code != 441110 -- remove car dealers
    AND
        fk_sgbrands IS NOT NULL  -- remove unbranded
