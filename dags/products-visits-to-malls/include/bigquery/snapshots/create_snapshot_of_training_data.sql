DECLARE date_str DATE;

DECLARE sql STRING;

SET
    date_str = DATE_ADD("{{ ds }}", INTERVAL 8 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}_snapshot`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}_snapshot` CLONE `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset'] }}.{{ params['visits_to_malls_training_data_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;