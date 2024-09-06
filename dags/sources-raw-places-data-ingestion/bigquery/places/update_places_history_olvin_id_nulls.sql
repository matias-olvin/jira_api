UPDATE 
    `{{ params['project'] }}.{{ params['staging_data_dataset']}}.{{ params['all_places_table'] }}`
SET 
  olvin_id=generate_uuid(),
  sg_id_first=sg_id
WHERE
  olvin_id IS NULL