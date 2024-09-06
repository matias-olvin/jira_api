DECLARE sg_table_suffix STRING;
DECLARE date_tag STRING;

SET sg_table_suffix = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    CAST(CURRENT_DATE() AS string format('MM')),
    '01'
);
SET date_tag = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    '-',
    CAST(CURRENT_DATE() AS string format('MM')),
    '-',
    '01'
);


INSERT INTO `{{ params['project'] }}.{{ params['metrics_data_dataset'] }}.{{ params['brand_metadata_table'] }}`
(
    local_date,
    count_brand_id,
    count_brand_name,
    count_parent_brand_id,
    count_naics_code,
    count_top_category,
    count_sub_category
)
SELECT
    CAST(date_tag AS DATE),
    COUNT(DISTINCT safegraph_brand_id),
    COUNT(DISTINCT brand_name),
    COUNT(DISTINCT parent_safegraph_brand_id),
    COUNT(DISTINCT naics_code),
    COUNT(DISTINCT top_category),
    COUNT(DISTINCT sub_category)
FROM 
    `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_brands` 