CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['public_feeds_finance_dataset'] }}.{{ params['store_visitors_table'] }}`
PARTITION BY month_starting
CLUSTER BY store_id
AS

SELECT feed.*, SGBrandRaw.stock_symbol, SGBrandRaw.stock_exchange 
FROM `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visitors_table'] }}` feed
LEFT JOIN `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` SGPlaceRaw ON feed.store_id=SGPlaceRaw.pid
LEFT JOIN `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgbrandraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` SGBrandRaw ON SGBrandRaw.pid=SGPlaceRaw.fk_Sgbrands
AND stock_symbol != ''
AND stock_symbol IS NOT null