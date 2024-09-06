CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 90 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgbrandraw_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgbrandraw_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 90 DAY));

CREATE TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['base_tables_dataset'] }}-{{ params['non_sensitive_naics_codes_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 90 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['places_dataset'] }}-{{ params['places_dynamic_table'] }}-{{ var.value.data_feed_places_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['places_dynamic_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 90 DAY));