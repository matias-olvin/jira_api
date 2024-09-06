CREATE OR REPLACE TABLE 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['spend_patterns_static_table'] }}`
PARTITION BY local_month
CLUSTER BY fk_sgplaces
AS (
    SELECT
        *
    FROM 
        `{{ var.value.env_project }}.{{ params['sg_places_staging_dataset'] }}.{{ params['spend_patterns_static_table'] }}`
)
