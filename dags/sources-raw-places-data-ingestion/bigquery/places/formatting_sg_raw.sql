CREATE OR REPLACE TABLE `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw` AS
    SELECT 
        * except(postal_code, opened_on, closed_on),
        LPAD(CAST(postal_code AS STRING), 5, '0') AS postal_code,
        PARSE_DATE('%Y-%m',opened_on) AS opened_on, 
        PARSE_DATE('%Y-%m',closed_on) AS closed_on 
    FROM 
        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw`
    WHERE 
        polygon_wkt IS NOT NULL