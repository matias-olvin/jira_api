ASSERT (
    SELECT
        COUNT(*)
    FROM
        (
            SELECT
                input.fk_sgplaces,
                input.name,
                input.region AS input_region,
                input.city,
                input.latitude,
                input.longitude,
                state.region AS region_from_state_table,
                state.pid AS region_id_from_state_table
            FROM
                `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}` input
                LEFT JOIN `{{ var.value.env_project }}.{{ params['area_geometries_dataset'] }}.{{ params['states_table'] }}` state ON ST_WITHIN(
                    ST_GEOGPOINT(input.longitude, input.latitude),
                    state.polygon
                )
        )
    WHERE
        region_from_state_table IS NULL
        OR region_id_from_state_table IS NULL
) = 0 AS "Latitude and longitude values may be wrong in {{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}: left join produced nulls in region or region_id";