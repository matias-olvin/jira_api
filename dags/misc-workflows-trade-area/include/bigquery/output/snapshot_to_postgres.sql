DECLARE date_str DATE;
DECLARE sql STRING;
SET date_str = DATE_ADD("{{ next_ds }}", INTERVAL 12 WEEK);

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table_postgres'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
AS
SELECT * from `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table'] }}`;

DROP TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacetradearearaw_table'] }}`;

SET sql= CONCAT("CREATE TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacetradearearaw_table'] }}` CLONE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table_postgres'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",date_str,"')");

EXECUTE IMMEDIATE sql;


TRUNCATE TABLE `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['processed_functional_areas_table_postgres'] }}`;

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplacetradearearaw_table'] }}`;
CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplacetradearearaw_table'] }}` CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplacetradearearaw_table'] }}` 