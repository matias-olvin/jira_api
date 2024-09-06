CREATE OR REPLACE TABLE `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_brands` AS
SELECT 
    * except(naics_code), 
    CAST(naics_code AS INT64) AS naics_code
FROM 
    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_brands`