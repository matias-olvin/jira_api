MERGE INTO `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}` AS all_data
    USING (
        SELECT DISTINCT 
            olvin_id,
            sg_id_active,
            sg_id_first
        FROM
            `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
        WHERE 
            olvin_id IS NOT NULL AND 
            active IS TRUE
    ) AS existing_olvin_ids
    ON all_data.sg_id_active = existing_olvin_ids.sg_id_active
    WHEN MATCHED THEN
        UPDATE SET
            olvin_id=existing_olvin_ids.olvin_id,
            sg_id_first=existing_olvin_ids.sg_id_first
    WHEN NOT MATCHED THEN
        INSERT (
            olvin_id,
            sg_id_first
        )
        VALUES (
            generate_uuid(),
            sg_id_active
        )