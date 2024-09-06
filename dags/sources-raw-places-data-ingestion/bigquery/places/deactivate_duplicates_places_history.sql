MERGE INTO
    `{{ params['project'] }}.{{ params['staging_data_dataset']}}.{{ params['all_places_table'] }}` places
USING (
    SELECT DISTINCT 
        fk_sgplaces 
    FROM
        `{{ params['project'] }}.{{ params['places_data_dataset']}}.{{ params['duplicates_table'] }}`
) duplicates
ON
    places.sg_id = duplicates.fk_sgplaces
WHEN MATCHED THEN
    UPDATE SET
        active = false