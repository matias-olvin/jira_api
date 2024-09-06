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
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_brands_table'] }}` a
LEFT JOIN 
    `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['brand_urls_table'] }}` b
ON
    a.pid = b.fk_sgbrands
WHERE 
    naics_code IN (
        SELECT 
            naics_code 
        FROM
            `{{ params['project'] }}.{{ params['base_tables_data_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}` 
    )
AND
    naics_code != 441120 AND naics_code != 441110 -- remove car dealers