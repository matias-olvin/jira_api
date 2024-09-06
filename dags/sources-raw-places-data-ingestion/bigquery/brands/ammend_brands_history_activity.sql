DECLARE run_date STRING;
SET run_date = CONCAT(
        CAST(CURRENT_DATE() AS STRING FORMAT('YYYY')),
        '-',
        CAST(CURRENT_DATE() AS STRING FORMAT('MM')),
        '-',
        '01'
);

UPDATE 
    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_brands_table'] }}`
    SET 
        activity = 'inactive'
    WHERE 
        last_seen < CAST(run_date AS DATE)
;
