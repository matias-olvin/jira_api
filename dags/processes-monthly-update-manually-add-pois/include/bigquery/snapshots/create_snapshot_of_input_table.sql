-- SNAPSHOT IS CREATED BEFORE INGESTING INPUT TABLE
DECLARE date_str DATE;

DECLARE sql STRING;

SET
    date_str = DATE_ADD("{{ ds }}", INTERVAL 6 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}_snapshot`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}_snapshot` CLONE `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;