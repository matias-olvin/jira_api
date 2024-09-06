CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` AS
SELECT
    fk_sgplaces AS pid,
    IFNULL(input.name, brand.name) AS name,
    fk_sgbrands,
    industry,
    top_category,
    sub_category,
    naics_code,
    input.latitude,
    input.longitude,
    street_address,
    open_hours,
    category_tags,
    IFNULL(enclosed, FALSE) AS enclosed,
    phone_number,
    TRUE AS is_synthetic,
    IFNULL(includes_parking_lot, FALSE) AS includes_parking_lot,
    'US' AS iso_country_code,
    fk_sgcenters AS fk_parents,
    opening_date,
    ST_AREA(ST_GEOGFROMTEXT(input.polygon)) * 10.764 AS polygon_area_sq_ft,
    city_id,
    fk_sgcenters,
    closing_date,
    IFNULL(city.city, input.city) AS city,
    IFNULL(state.region, input.region) AS region,
    IFNULL(LPAD(CAST(zipcodes.pid AS STRING), 5, '0'), input.postal_code) AS postal_code,
    IF(closing_date IS NULL, 1, 0) AS opening_status
FROM
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}` input
    LEFT JOIN `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` brand ON fk_sgbrands = pid
    LEFT JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['states_table'] }}` state ON ST_WITHIN(
        ST_GEOGPOINT(input.longitude, input.latitude),
        state.polygon
    )
    LEFT JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['city_table'] }}` city ON ST_WITHIN(
        ST_GEOGPOINT(input.longitude, input.latitude),
        city.polygon
    )
    LEFT JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['zipcodes_table'] }}` zipcodes ON ST_WITHIN(
        ST_GEOGPOINT(input.longitude, input.latitude),
        ST_GEOGFROMTEXT(zipcodes.polygon)
    )
WHERE
    added_date >= CAST(
        '{{ var.value.manually_add_pois_deadline_date }}' AS DATE
    )
    AND fk_sgbrands IN (
        SELECT pid
        FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}`
    );