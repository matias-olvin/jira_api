CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}` 
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
            `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_history_table'] }}` 
        WHERE 
            activity = 'active'
