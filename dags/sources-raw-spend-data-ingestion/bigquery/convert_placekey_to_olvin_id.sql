WITH 
    placekey_to_olvin_map AS (
        SELECT 
            sg_id, 
            olvin_id
        FROM 
            `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['all_places_table'] }}`
    )

SELECT 
    map.olvin_id AS fk_sgplaces,
    spend.* EXCEPT(placekey, parent_placekey)
FROM 
    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['spend_patterns_raw_table'] }}` spend
INNER JOIN 
    placekey_to_olvin_map map
ON
    map.sg_id = spend.placekey
