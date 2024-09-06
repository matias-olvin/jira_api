DECLARE date_tag STRING;
SET date_tag = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    '-',
    CAST(CURRENT_DATE() AS string format('MM')),
    '-',
    '01'
);

INSERT INTO `{{ params['project'] }}.{{ params['metrics_data_dataset'] }}.{{ params['raw_metadata_US_table'] }}`
    (
        local_date,
        count_US_placekey,
        count_US_city,
        count_US_region,
        count_US_postal_code,
        count_US_polygon_wkt,
        count_US_closed_on,
        count_US_branded,
        count_US_opened_on,
        count_US_open_branded,
        count_US_open_opened_on
    )
    SELECT
        CAST(date_tag AS DATE),
        COUNT(IF(iso_country_code='US', placekey, NULL)),
        COUNT(IF(iso_country_code='US', city, NULL)), 
        COUNT(IF(iso_country_code='US', region, NULL)), 
        COUNT(IF(iso_country_code='US', postal_code, NULL)),
        COUNT(IF(iso_country_code='US', polygon_wkt, NULL)), 
        COUNT(IF(iso_country_code='US', closed_on, NULL)),
        COUNT(IF(iso_country_code='US', brands, NULL)),
        COUNT(IF(iso_country_code='US', opened_on, NULL)),
        COUNT(IF(iso_country_code='US' and closed_on is NULL, brands, NULL)),
        COUNT(IF(iso_country_code='US' and closed_on is NULL, opened_on, NULL))
    FROM 
        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_raw` 