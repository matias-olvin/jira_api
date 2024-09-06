DECLARE date_str DATE;

DECLARE sql STRING;

CREATE TABLE IF NOT EXISTS
    `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}` (
        naics_code STRING,
        top_category STRING,
        sub_category STRING,
        sensitive BOOLEAN
    );


SET
    date_str = DATE_ADD("{{ ds }}", INTERVAL 14 WEEK);

DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}_snapshot`;

SET
    sql = CONCAT(
        "CREATE SNAPSHOT TABLE IF NOT EXISTS `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}_snapshot` CLONE `{{ var.value.env_project }}.{{ params['sg_base_tables_staging_dataset'] }}.{{ params['new_naics_codes_sensitivity_check_table'] }}` OPTIONS (expiration_timestamp = TIMESTAMP '",
        date_str,
        "')"
    );

EXECUTE IMMEDIATE sql;