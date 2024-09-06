CREATE OR REPLACE TABLE  `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_brands_table'] }}` 
    AS
        SELECT 
            * EXCEPT(_row_num) 
        FROM (
            SELECT 
                ROW_NUMBER() OVER(PARTITION BY pid ORDER BY first_seen, last_seen DESC) AS _row_num,
                *
            FROM 
                `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.{{ params['all_brands_table'] }}`
        )
        WHERE 
            _row_num = 1