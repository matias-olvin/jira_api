MERGE INTO 
    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}` all_data
USING 
    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.lineage` lineage
ON 
    all_data.sg_id=lineage.placekey
WHEN MATCHED THEN
    UPDATE SET
        sg_id_active=IFNULL(lineage.current_placekey, lineage.placekey),
        active=lineage.active
    