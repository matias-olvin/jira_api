DECLARE date_tag STRING;
SET date_tag = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    '-',
    CAST(CURRENT_DATE() AS string format('MM')),
    '-',
    '01'
);

INSERT INTO `{{ params['project'] }}.{{ params['metrics_data_dataset'] }}.{{ params['places_placekey_stats_table'] }}`
    (
        local_date,
        total_placekeys,
        new_placekeys,
        active_placekeys,
        inactive_placekeys
    )
    WITH 
        total AS (
            SELECT 
                COUNT(sg_id) as total
            FROM 
                `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places`
        ),
        active_count AS (
            SELECT 
                COUNT(*) as total
            FROM 
                `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_places_table'] }}`
            WHERE 
                active IS TRUE
        ),
        inactive_count AS (
            SELECT 
                COUNT(*) as total
            FROM 
                `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_places_table'] }}`
            WHERE 
                active IS FALSE
        ),
        new_placekeys AS (
            SELECT 
                COUNT(*) AS total
            FROM 
                `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_places_table'] }}`
            WHERE 
                first_seen = CAST(date_tag AS DATE) AND 
                last_seen = CAST(date_tag AS DATE)
        )
    SELECT 
        CAST(date_tag AS DATE) as local_date, 
        total.total AS total_placekeys, 
        new_placekeys.total AS new_placekeys, 
        active_count.total AS active_placekeys, 
        inactive_count.total AS inactive_placekeys
    FROM 
        total, 
        new_placekeys, 
        active_count, 
        inactive_count
