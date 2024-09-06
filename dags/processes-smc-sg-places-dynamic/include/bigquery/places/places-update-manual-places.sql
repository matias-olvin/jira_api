CREATE TEMP TABLE
    info_to_insert AS
WITH

inactive_brands AS( -- Check for possible parenthoods of inactive brands to update them
  SELECT *
  FROM(
    SELECT pid AS inactive_fk_sgbrands, fk_parents AS amended_fk_sgbrands
    FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_history_table'] }}`
    WHERE activity='inactive'
  )
  INNER JOIN(
    SELECT pid AS amended_fk_sgbrands, name AS amended_brand
    FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
  )
  USING(amended_fk_sgbrands)
),

    -- Very careful to maintain the same order and data type as of the destination table
    formatted_info AS (
        SELECT
            GENERATE_UUID() AS pid, -- GENERATE_UUID()
            fk_sgbrands,
            brand.name AS brands,
            brand.top_category,
            brand.sub_category,
            brand.naics_code,
            TO_HEX(
                CAST(
                    (
                        SELECT
                            STRING_AGG(
                                CAST(
                                    S2_CELLIDFROMPOINT(
                                        ST_GEOGPOINT(input.longitude, input.latitude),
                                        level => 20
                                    ) >> bit & 0x1 AS STRING
                                ),
                                ''
                                ORDER BY
                                    bit DESC
                            )
                        FROM
                            UNNEST (GENERATE_ARRAY(0, 63)) AS bit
                    ) AS BYTES FORMAT "BASE2"
                )
            ) AS site_id, -- Use S2Level 20
            input.latitude,
            input.longitude,
            street_address,
            IFNULL(city.city, input.city) AS city,
            state.region AS region,
            IFNULL(
                LPAD(CAST(zipcodes.pid AS STRING), 5, '0'),
                input.postal_code
            ) AS postal_code,
            category_tags,
            'MANUAL_POLYGON' AS polygon_class,
            IFNULL(enclosed, FALSE) AS enclosed,
            'US' AS iso_country_code,
            state.pid AS region_id,
            ST_GEOGPOINT(input.longitude, input.latitude) AS long_lat_point,
            NULLIF(fk_parents, '') AS fk_parents,
            IF(NULLIF(fk_parents, '') IS NULL, TRUE, FALSE) AS standalone_bool,
            IF(NULLIF(fk_parents, '') IS NULL, FALSE, TRUE) AS child_bool,
            FALSE AS parent_bool,
            ST_AREA(ST_GEOGFROMTEXT(input.polygon)) * 10.764 AS polygon_area_sq_ft,
            industry,
            tzid AS timezone,
            phone_number,
            TRUE AS is_synthetic,
            IFNULL(includes_parking_lot, FALSE) AS includes_parking_lot,
            opening_date,
            closing_date,
            CONCAT(brand.name, ', ', street_address) AS name,
            ST_GEOGFROMTEXT(input.polygon) AS polygon_wkt,
            ST_GEOGFROMTEXT(input.polygon) AS simplified_polygon_wkt,
            ST_BUFFER(ST_GEOGFROMTEXT(input.polygon), 10) AS simplified_wkt_10_buffer,
            NULLIF(fk_parents, '') AS fk_parents_override,
            IF(NULLIF(fk_parents, '') IS NULL, TRUE, FALSE) AS standalone_bool_override,
            IF(NULLIF(fk_parents, '') IS NULL, FALSE, TRUE) AS child_bool_override,
            FALSE AS parent_bool_override,
            open_hours
        FROM(
          SELECT * EXCEPT(fk_sgbrands),
                 IFNULL(amended_fk_sgbrands, fk_sgbrands) AS fk_sgbrands
          FROM `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}`
          LEFT JOIN inactive_brands -- Update inactive fk_sgbrand based on parenthood relationship
            ON fk_sgbrands = inactive_fk_sgbrands
        ) input
            INNER JOIN `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}` brand ON fk_sgbrands = brand.pid
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
            LEFT JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['worldwide_timezones_table'] }}` timezones ON ST_WITHIN(
                ST_GEOGPOINT(input.longitude, input.latitude),
                timezones.geometry
            )
        WHERE
            added_date >= CAST(
                '{{ var.value.manually_add_pois_deadline_date }}' AS DATE
            )
            AND added_date < CAST('{{ var.value.smc_start_date }}' AS DATE)
    )
SELECT
    formatted_info.*
FROM
    formatted_info
    LEFT JOIN (
        SELECT
            fk_sgbrands,
            long_lat_point,
            TRUE AS in_pd
        FROM
            `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
    ) places_dynamic ON places_dynamic.fk_sgbrands = formatted_info.fk_sgbrands
    AND ST_DISTANCE(
        places_dynamic.long_lat_point,
        formatted_info.long_lat_point
    ) < 200
WHERE
    in_pd IS NULL;

MERGE INTO
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_manual_table'] }}` AS target USING info_to_insert AS source ON target.fk_sgbrands = source.fk_sgbrands
    AND ST_DISTANCE(target.long_lat_point, source.long_lat_point) < 200
WHEN MATCHED THEN
UPDATE SET
    pid = source.pid,
    fk_sgbrands = source.fk_sgbrands,
    brands = source.brands,
    top_category = source.top_category,
    sub_category = source.sub_category,
    naics_code = source.naics_code,
    site_id = source.site_id,
    latitude = source.latitude,
    longitude = source.longitude,
    street_address = source.street_address,
    city = source.city,
    region = source.region,
    postal_code = source.postal_code,
    category_tags = source.category_tags,
    polygon_class = source.polygon_class,
    enclosed = source.enclosed,
    iso_country_code = source.iso_country_code,
    region_id = source.region_id,
    long_lat_point = source.long_lat_point,
    fk_parents = source.fk_parents,
    standalone_bool = source.standalone_bool,
    child_bool = source.child_bool,
    parent_bool = source.parent_bool,
    polygon_area_sq_ft = source.polygon_area_sq_ft,
    industry = source.industry,
    timezone = source.timezone,
    phone_number = source.phone_number,
    is_synthetic = source.is_synthetic,
    includes_parking_lot = source.includes_parking_lot,
    opening_date = source.opening_date,
    closing_date = source.closing_date,
    name = source.name,
    polygon_wkt = source.polygon_wkt,
    simplified_polygon_wkt = source.simplified_polygon_wkt,
    simplified_wkt_10_buffer = source.simplified_wkt_10_buffer,
    fk_parents_override = source.fk_parents_override,
    standalone_bool_override = source.standalone_bool_override,
    child_bool_override = source.child_bool_override,
    parent_bool_override = source.parent_bool_override,
    open_hours = source.open_hours
WHEN NOT MATCHED THEN
INSERT
    (
        pid,
        fk_sgbrands,
        brands,
        top_category,
        sub_category,
        naics_code,
        site_id,
        latitude,
        longitude,
        street_address,
        city,
        region,
        postal_code,
        category_tags,
        polygon_class,
        enclosed,
        iso_country_code,
        region_id,
        long_lat_point,
        fk_parents,
        standalone_bool,
        child_bool,
        parent_bool,
        polygon_area_sq_ft,
        industry,
        timezone,
        phone_number,
        is_synthetic,
        includes_parking_lot,
        opening_date,
        closing_date,
        name,
        polygon_wkt,
        simplified_polygon_wkt,
        simplified_wkt_10_buffer,
        fk_parents_override,
        standalone_bool_override,
        child_bool_override,
        parent_bool_override,
        open_hours
    )
VALUES
    (
        source.pid,
        source.fk_sgbrands,
        source.brands,
        source.top_category,
        source.sub_category,
        source.naics_code,
        source.site_id,
        source.latitude,
        source.longitude,
        source.street_address,
        source.city,
        source.region,
        source.postal_code,
        source.category_tags,
        source.polygon_class,
        source.enclosed,
        source.iso_country_code,
        source.region_id,
        source.long_lat_point,
        source.fk_parents,
        source.standalone_bool,
        source.child_bool,
        source.parent_bool,
        source.polygon_area_sq_ft,
        source.industry,
        source.timezone,
        source.phone_number,
        source.is_synthetic,
        source.includes_parking_lot,
        source.opening_date,
        source.closing_date,
        source.name,
        source.polygon_wkt,
        source.simplified_polygon_wkt,
        source.simplified_wkt_10_buffer,
        source.fk_parents_override,
        source.standalone_bool_override,
        source.child_bool_override,
        source.parent_bool_override,
        source.open_hours
    )