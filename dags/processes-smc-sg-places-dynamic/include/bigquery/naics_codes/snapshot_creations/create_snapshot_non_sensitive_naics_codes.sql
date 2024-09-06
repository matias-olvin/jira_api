DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sg_base_tables_dataset'] }}-{{ params['non_sensitive_naics_codes_table'] }}-{{ ds }}`;

CREATE SNAPSHOT TABLE IF NOT EXISTS
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['sg_base_tables_dataset'] }}-{{ params['non_sensitive_naics_codes_table'] }}-{{ ds }}` CLONE `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}`;