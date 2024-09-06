CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplacedailyvisitsraw_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['cameo_staging_dataset'] }}-{{ params['smoothed_transition_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['cameo_staging_dataset'] }}.{{ params['smoothed_transition_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_processed_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['static_demographics_data_dataset'] }}.{{ params['cameo_categories_processed_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['static_demographics_data_dataset'] }}-{{ params['cameo_categories_percentages_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['static_demographics_data_dataset'] }}.{{ params['cameo_categories_percentages_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['destinations_dataset'] }}-{{ params['destinations_history_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['destinations_dataset'] }}.{{ params['destinations_history_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['home_dataset'] }}-{{ params['zipcodes_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['home_dataset'] }}.{{ params['zipcodes_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceactivity_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));