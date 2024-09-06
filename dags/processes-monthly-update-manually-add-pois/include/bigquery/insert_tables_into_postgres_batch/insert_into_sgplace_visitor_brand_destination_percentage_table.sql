MERGE INTO
    `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplace_visitor_brand_destination_percentage_table'] }}` AS target USING `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplace_visitor_brand_destination_percentage_table'] }}` AS source ON target.pid = source.pid
WHEN MATCHED THEN
UPDATE SET
    pid = source.pid,
    brands = source.brands,
    total_devices = source.total_devices
WHEN NOT MATCHED THEN
INSERT
    (pid, brands, total_devices)
VALUES
    (source.pid, source.brands, source.total_devices);