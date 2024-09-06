DECLARE date_tag STRING;
SET date_tag = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    '-',
    CAST(CURRENT_DATE() AS STRING format('MM')),
    '-',
    '01'
);

MERGE INTO 
        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_brands_table'] }}` all_data
    USING (
        SELECT *
        FROM 
            `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_brands` 
        WHERE 
            iso_country_codes_open LIKE '[%"US"%]'
    ) AS brands
        ON 
            all_data.pid=brands.safegraph_brand_id 
    WHEN MATCHED THEN
        UPDATE SET 
            brand=brands.brand_name,
            fk_parents=brands.parent_safegraph_brand_id,
            naics_code=brands.naics_code,
            top_category=brands.top_category,
            sub_category=brands.sub_category,
            stock_symbol=brands.stock_symbol,
            stock_exchange=brands.stock_exchange,
            last_seen=CAST(date_tag as DATE),
            activity="active"
    WHEN NOT MATCHED THEN
        INSERT (
            pid,
            brand,
            fk_parents,
            naics_code,
            top_category,
            sub_category,
            stock_symbol,
            stock_exchange,
            first_seen,
            last_seen,
            activity
        )
        VALUES(
            brands.safegraph_brand_id,
            brands.brand_name,
            brands.parent_safegraph_brand_id,
            brands.naics_code,
            brands.top_category,
            brands.sub_category,
            brands.stock_symbol,
            brands.stock_exchange,
            CAST(date_tag as DATE),
            CAST(date_tag as DATE),
            "active"
        )