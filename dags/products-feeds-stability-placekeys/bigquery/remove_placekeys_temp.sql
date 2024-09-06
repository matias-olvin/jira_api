DELETE FROM 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['placekeys_temp_table'] }}`
WHERE store_id NOT IN (
    SELECT store_id 
    FROM 
        `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_table'] }}`
    UNION DISTINCT
    SELECT store_id 
    FROM 
        `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_trend_table'] }}`
    UNION DISTINCT
    SELECT store_id 
    FROM 
        `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visitors_table'] }}`
)
