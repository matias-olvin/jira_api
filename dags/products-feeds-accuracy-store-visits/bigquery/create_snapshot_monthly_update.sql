CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplacedailyvisitsraw_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['hourly_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['hourly_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));

CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['postgres_batch_dataset'] }}-{{ params['sgplaceactivity_table'] }}-{{ var.value.data_feed_data_version.replace('.', '-') }}`
CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceactivity_table'] }}` OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(),INTERVAL 30 DAY));