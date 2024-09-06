CREATE OR REPLACE TABLE `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['dynamic_brands_table'] }}` 
    AS 
        SELECT 
            pid, 
            brand AS name, 
            fk_parents,
            naics_code, 
            top_category,
            sub_category,
            stock_symbol,
            stock_exchange,
            first_seen,
            last_seen,
            activity
        FROM 
            `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_brands_table'] }}` 
        WHERE 
            activity = 'active'