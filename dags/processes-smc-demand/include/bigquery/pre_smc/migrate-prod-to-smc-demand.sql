CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['quality_dataset'] }}_{{ params['model_input_complete_table'] }}`
COPY `{{ var.value.env_project }}.{{ params['quality_dataset'] }}.{{ params['model_input_complete_table'] }}`;


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['daily_estimation_dataset'] }}_{{ params['daily_factor_table'] }}`
COPY `{{ var.value.env_project }}.{{ params['daily_estimation_dataset'] }}.{{ params['daily_factor_table'] }}`;


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['daily_estimation_dataset'] }}_{{ params['grouped_daily_olvin_table'] }}`
COPY `{{ var.value.env_project }}.{{ params['daily_estimation_dataset'] }}.{{ params['grouped_daily_olvin_table'] }}`;


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_dataset'] }}`
PARTITION BY local_date
CLUSTER BY lat_long_visit_point, country, fk_sgplaces, device_id
AS 
SELECT * FROM
`{{ var.value.env_project }}.{{ params['poi_visits_dataset'] }}.*`
WHERE fk_sgplaces IN 
(SELECT pid FROM 
`{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['places_demand_table'] }}`
);