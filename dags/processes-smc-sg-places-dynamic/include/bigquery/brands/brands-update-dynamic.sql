UPDATE 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
SET 
    naics_code = 452311, 
    top_category = 'General Merchandise Stores, including Warehouse Clubs and Supercenters',
    sub_category = 'Warehouse Clubs and Supercenters'
WHERE 
    pid IN (
        'SG_BRAND_98f6b474794d85f694762c9b52bbc351',-- Kmart
        'SG_BRAND_c4d65e0c51ea2fe6cef625aabba41ab6' --Big Lots
    )
