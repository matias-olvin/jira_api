DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['smc_sg_places_dataset'] }}-{{ params['different_digit_naics_code_table'] }}-{{ ds }}`;

CREATE SNAPSHOT TABLE IF NOT EXISTS
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['smc_sg_places_dataset'] }}-{{ params['different_digit_naics_code_table'] }}-{{ ds }}` CLONE `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['different_digit_naics_code_table'] }}`;