DECLARE date_str DATE;
DECLARE sql STRING;
SET date_str = DATE_ADD("{{ next_ds }}", INTERVAL 2 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_snapshot_table'] }}`;

SET sql= CONCAT("CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_snapshot_table'] }}` CLONE `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",date_str,"')");

EXECUTE IMMEDIATE sql;

TRUNCATE TABLE `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_table'] }}`;

INSERT INTO `{{ var.value.env_project }}.{{ params['public_feeds_dataset'] }}.{{ params['store_visits_table'] }}`
SELECT * FROM `{{ var.value.env_project }}.{{ params['public_feeds_staging_dataset'] }}.{{ params['store_visits_temp_table'] }}`;