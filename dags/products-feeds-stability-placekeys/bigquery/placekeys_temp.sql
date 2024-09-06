CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['placekeys_temp_table'] }}` 
AS

SELECT 
  t1.pid AS store_id
  , t2.sg_id AS placekey
  , t1.name
  , t1.street_address 
FROM 
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` t1
JOIN
  `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['places_dataset'] }}-{{ params['places_history_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}` t2
ON t1.pid = t2.olvin_id
