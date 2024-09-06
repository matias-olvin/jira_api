-- SNAPSHOT IS CREATED FOR EVERY POSTGRES FINAL TABLE BEFORE BEING INSERTED INTO
DECLARE date_str DATE;

DECLARE sql STRING;

SET
    date_str = DATE_ADD("{{ ds }}", INTERVAL 6 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['manually_add_pois_dataset'] }}-{{ table_name }}_snapshot`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['manually_add_pois_dataset'] }}-{{ table_name }}_snapshot` CLONE `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ table_name }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;