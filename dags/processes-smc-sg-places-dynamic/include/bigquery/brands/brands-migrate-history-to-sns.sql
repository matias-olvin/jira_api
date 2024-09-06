CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_history_table'] }}`
COPY
    `{{ var.value.env_project }}.{{ params['sg_places_staging_dataset'] }}.{{ params['brands_history_table'] }}`
