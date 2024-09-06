DECLARE date_str DATE;

DECLARE sql STRING;

SET
    date_str = DATE_ADD("{{ ds }}", INTERVAL 14 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sensitive_naics_codes_table'] }}_snapshot`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sensitive_naics_codes_table'] }}_snapshot` CLONE `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sensitive_naics_codes_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;