DECLARE date_tag STRING;
SET date_tag = CONCAT(
    CAST(CURRENT_DATE() AS STRING format('YYYY')),
    '-',
    CAST(CURRENT_DATE() AS string format('MM')),
    '-',
    '01'
);

INSERT INTO `{{ params['project'] }}.{{ params['metrics_data_dataset'] }}.{{ params['places_metadata_table'] }}`
    (
        local_date,
        count_placekey, 
        count_city, 
        count_region, 
        count_postal_code,
        count_closed_on,
        count_opened_on, 
        count_polygon_wkt, 
        count_iso_code
    )
    SELECT
        CAST(date_tag AS DATE) AS local_date,
        COUNT(DISTINCT sg_id) AS count_placekey, 
        COUNT(DISTINCT city) AS count_city, 
        COUNT(DISTINCT region) AS count_region, 
        COUNT(DISTINCT postal_code) AS count_postal_code,
        COUNT(closed_on) AS count_closed_on,
        COUNT(opened_on) AS count_opened_on, 
        COUNT(polygon_wkt) AS count_polygon_wkt, 
        COUNT(DISTINCT iso_country_code) AS count_iso_code
    FROM 
        `{{ params['project'] }}.{{ params['staging_data_dataset'] }}.sg_places`