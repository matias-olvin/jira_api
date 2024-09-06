MERGE INTO
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` AS target USING `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}` AS source ON target.pid = source.pid
WHEN MATCHED THEN
UPDATE SET
    pid = source.pid,
    name = source.name,
    fk_sgbrands = source.fk_sgbrands,
    industry = source.industry,
    top_category = source.top_category,
    sub_category = source.sub_category,
    naics_code = source.naics_code,
    latitude = source.latitude,
    longitude = source.longitude,
    street_address = source.street_address,
    region = source.region,
    open_hours = source.open_hours,
    category_tags = source.category_tags,
    enclosed = source.enclosed,
    phone_number = source.phone_number,
    is_synthetic = source.is_synthetic,
    includes_parking_lot = source.includes_parking_lot,
    iso_country_code = source.iso_country_code,
    fk_parents = source.fk_parents,
    opening_date = source.opening_date,
    polygon_area_sq_ft = source.polygon_area_sq_ft,
    city_id = source.city_id,
    city = source.city,
    postal_code = source.postal_code,
    fk_sgcenters = source.fk_sgcenters,
    closing_date = source.closing_date,
    opening_status = source.opening_status
WHEN NOT MATCHED THEN
INSERT
    (
        pid,
        name,
        fk_sgbrands,
        industry,
        top_category,
        sub_category,
        naics_code,
        latitude,
        longitude,
        street_address,
        region,
        open_hours,
        category_tags,
        enclosed,
        phone_number,
        is_synthetic,
        includes_parking_lot,
        iso_country_code,
        fk_parents,
        opening_date,
        polygon_area_sq_ft,
        city_id,
        city,
        postal_code,
        fk_sgcenters,
        closing_date,
        opening_status
    )
VALUES
    (
        source.pid,
        source.name,
        source.fk_sgbrands,
        source.industry,
        source.top_category,
        source.sub_category,
        source.naics_code,
        source.latitude,
        source.longitude,
        source.street_address,
        source.region,
        source.open_hours,
        source.category_tags,
        source.enclosed,
        source.phone_number,
        source.is_synthetic,
        source.includes_parking_lot,
        source.iso_country_code,
        source.fk_parents,
        source.opening_date,
        source.polygon_area_sq_ft,
        source.city_id,
        source.city,
        source.postal_code,
        source.fk_sgcenters,
        source.closing_date,
        source.opening_status
    );