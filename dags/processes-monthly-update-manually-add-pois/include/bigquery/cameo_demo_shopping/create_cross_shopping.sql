CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplace_visitor_brand_destination_table'] }}` AS
WITH
    poi_data AS (
        SELECT
            pid AS fk_sgplaces,
            fk_sgbrands,
            ST_GEOGPOINT(longitude, latitude) AS poi_geoloc,
            region
        FROM
            `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    ),
    brand_pois AS (
        SELECT
            poi_to_add,
            pid AS fk_sgplaces
        FROM
            `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
            INNER JOIN (
                SELECT
                    fk_sgplaces AS poi_to_add,
                    fk_sgbrands,
                    region
                FROM
                    poi_data
            ) USING (fk_sgbrands, region)
    ),
    -- Get equivalent info from SGPlaceVisitorBrandDestinations for pois of the same brand in the same state
    observed_connections AS (
        SELECT
            *
        FROM
            `{{ var.value.env_project }}.{{ params['visitor_brand_destinations_dataset'] }}.{{ params['observed_connections_table'] }}`
            INNER JOIN brand_pois USING (fk_sgplaces)
            INNER JOIN (
                SELECT
                    pid AS fk_sgbrands,
                    naics_code
                FROM
                    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
            ) USING (fk_sgbrands)
        WHERE
            SUBSTR(CAST(naics_code AS STRING), 1, 2) NOT IN (
                '11',
                '21',
                '22',
                '23',
                '33',
                '48',
                '49',
                '54',
                '55',
                '56',
                '61',
                '62',
                '92'
            )
            -- Exclude four-digit NAICS code prefixes
            AND SUBSTR(CAST(naics_code AS STRING), 1, 4) NOT IN ('5311', '5312')
    ),
    num_of_devices AS (
        SELECT
            poi_to_add,
            fk_sgplaces,
            ANY_VALUE(total_devices) AS total_devices
        FROM
            observed_connections
        GROUP BY
            poi_to_add,
            fk_sgplaces
    ),
    -- Get nearest poi per each brand appearing in the connections table
    brands_to_check_nearest_poi AS (
        SELECT DISTINCT
            poi_to_add,
            fk_sgbrands
        FROM
            observed_connections
    ),
    nearest_pois AS (
        SELECT
            poi_to_add,
            fk_sgbrands,
            MIN(distance) AS distance
        FROM
            (
                SELECT
                    poi_to_add,
                    fk_sgbrands,
                    fk_sgplaces,
                    ST_DISTANCE(poi_geoloc, geo_loc) AS distance
                FROM
                    brands_to_check_nearest_poi
                    INNER JOIN (
                        SELECT
                            poi_data.fk_sgplaces AS poi_to_add,
                            region,
                            poi_geoloc
                        FROM
                            poi_data
                    ) USING (poi_to_add)
                    INNER JOIN (
                        SELECT
                            pid AS fk_sgplaces,
                            ST_GEOGPOINT(longitude, latitude) AS geo_loc,
                            fk_sgbrands,
                            region
                        FROM
                            `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                    ) USING (fk_sgbrands, region)
                WHERE
                    ST_DISTANCE(poi_geoloc, geo_loc) < 50000 -- Remove brands which are at more than 50 km (should have very few visits)
            )
        GROUP BY
            poi_to_add,
            fk_sgbrands
    ),
    -- Adjusted observed connections: 
    --     - agg the ones of other pois with the same brand on the same state
    --     - apply factor based on nearest poi distance of the corresponding brand
    --         - factor >= 1 for pois at less than 2 km
    --         - factor = 0 for pois at more than 50 km
    --         - max(factor) = 1.26 for pois at 0 distance
    --         - min(factor) = 0.0625 for pois at 50 km
    agg_weigthed_observed_conns AS (
        SELECT
            poi_to_add,
            fk_sgbrands,
            CAST(
                shared_devices * POW(2, - (nearest_pois.distance - 2000) / 6000) AS INT64
            ) AS shared_devices
        FROM
            (
                SELECT
                    poi_to_add,
                    fk_sgbrands,
                    SUM(shared_devices) AS shared_devices
                FROM
                    observed_connections
                GROUP BY
                    poi_to_add,
                    fk_sgbrands
            )
            INNER JOIN nearest_pois USING (poi_to_add, fk_sgbrands)
    ),
    agg_num_of_devices AS (
        SELECT
            poi_to_add,
            SUM(total_devices) AS total_devices
        FROM
            num_of_devices
        GROUP BY
            poi_to_add
    )
    -- Formatted Table
SELECT
    poi_to_add AS pid,
    brands,
    total_devices
FROM
    (
        SELECT
            poi_to_add,
            REPLACE(
                REPLACE(
                    TO_JSON_STRING(ARRAY_AGG(STRUCT (fk_sgbrands, shared_devices))),
                    ',"shared_devices"',
                    ''
                ),
                '"fk_sgbrands":',
                ''
            ) AS brands
        FROM
            agg_weigthed_observed_conns
        WHERE
            shared_devices > 0
        GROUP BY
            poi_to_add
    )
    INNER JOIN agg_num_of_devices USING (poi_to_add)