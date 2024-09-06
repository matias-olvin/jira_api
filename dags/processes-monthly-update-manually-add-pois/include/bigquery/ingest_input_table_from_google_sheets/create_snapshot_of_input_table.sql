DROP SNAPSHOT TABLE IF EXISTS `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['smc_sg_places_dataset'] }}-{{ params['manually_add_pois_input_table'] }}-{{ ds }}`;

CREATE SNAPSHOT TABLE IF NOT EXISTS
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['smc_sg_places_dataset'] }}-{{ params['manually_add_pois_input_table'] }}-{{ ds }}` CLONE `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['manually_add_pois_input_table'] }}`;