MERGE INTO 
    `{{ params['project'] }}.{{ params['staging_data_dataset']}}.{{ params['all_places_table'] }}` places
USING (
    SELECT DISTINCT a.sg_id, b.olvin_id
    FROM 
        `{{ params['project'] }}.{{ params['staging_data_dataset']}}.{{ params['all_places_table'] }}` a
    LEFT JOIN 
        `{{ params['project'] }}.{{ params['staging_data_dataset']}}.{{ params['all_places_table'] }}` b
    ON a.fk_parents_sg = b.sg_id
    ) filtered
ON places.sg_id = filtered.sg_id
WHEN MATCHED THEN 
    UPDATE SET 
        fk_parents_olvin = filtered.olvin_id