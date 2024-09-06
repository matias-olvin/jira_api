DECLARE date_tag STRING;
SET date_tag = CONCAT(
        CAST(CURRENT_DATE() AS STRING FORMAT('YYYY')),
        '-',
        CAST(CURRENT_DATE() AS STRING FORMAT('MM')),
        '-',
        '01'
);

UPDATE 
    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_places_table'] }}`
SET
    site_id = TO_HEX(
        CAST(
            (
                SELECT
                    STRING_AGG(
                        CAST(
                            S2_CELLIDFROMPOINT(
                                long_lat_point, 
                                level => 20
                            ) >> bit & 0x1 AS STRING
                        ), '' ORDER BY bit DESC
                    ) 
                FROM 
                    UNNEST(GENERATE_ARRAY(0, 63)) AS bit 
            ) AS BYTES FORMAT "BASE2" 
        )
    )
WHERE 
    last_seen = CAST(date_tag AS DATE)