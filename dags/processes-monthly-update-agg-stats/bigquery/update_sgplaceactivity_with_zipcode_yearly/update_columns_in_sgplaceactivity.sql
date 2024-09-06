CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sg_place_activity_table'] }}` AS
SELECT
    * EXCEPT (home_locations),
    CASE
        WHEN fk_sgplaces IN (
            SELECT
                fk_sgplaces
            FROM
                `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sg_placehome_zipcode_yearly_table'] }}`
        ) THEN TRUE
        ELSE FALSE
    END AS home_locations,
    CASE
        WHEN EXISTS (
            SELECT
                1
            FROM
                `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplacetradearearaw_table'] }}` SGPlaceTradeAreaRaw
            WHERE
                SGPlaceTradeAreaRaw.fk_sgplaces = SGPlaceActivity.fk_sgplaces
        ) THEN TRUE
        ELSE FALSE
    END AS trade_area_activity,
    CASE
        WHEN EXISTS (
            SELECT
                1
            FROM
                `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sg_place_visitor_brand_destinations_table'] }}` destinations
            WHERE
                destinations.pid = SGPlaceActivity.fk_sgplaces
                AND total_devices >= 30
        ) THEN TRUE
        ELSE FALSE
    END AS cross_shopping_activity,
FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sg_place_activity_table'] }}` SGPlaceActivity;