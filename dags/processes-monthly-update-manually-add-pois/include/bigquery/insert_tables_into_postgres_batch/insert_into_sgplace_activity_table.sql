MERGE INTO
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplace_activity_table'] }}` AS target USING `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplace_activity_table'] }}` AS source ON target.fk_sgplaces = source.fk_sgplaces
WHEN MATCHED THEN
UPDATE SET
    fk_sgplaces = source.fk_sgplaces,
    activity = source.activity,
    confidence_level = source.confidence_level,
    home_locations = source.home_locations,
    connections = source.connections,
    trade_area_activity = source.trade_area_activity,
    cross_shopping_activity = source.cross_shopping_activity
WHEN NOT MATCHED THEN
INSERT
    (
        fk_sgplaces,
        activity,
        confidence_level,
        home_locations,
        connections,
        trade_area_activity,
        cross_shopping_activity
    )
VALUES
    (
        source.fk_sgplaces,
        source.activity,
        source.confidence_level,
        source.home_locations,
        source.connections,
        source.trade_area_activity,
        source.cross_shopping_activity
    );