INSERT INTO `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`
AS
SELECT * FROM `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['metrics_dataset'] }}_{{ params['day_stats_visits_scaled_table'] }}`;

INSERT INTO `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.2018`
AS
SELECT * FROM `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_scaled_dataset'] }`
WHERE local_date >= "2018-01-01" AND local_date <="2018-12-31";

INSERT INTO `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.2019`
AS
SELECT * FROM `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_scaled_dataset'] }`
WHERE local_date >= "2019-01-01" AND local_date <="2019-12-31";

INSERT INTO `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.2020`
AS
SELECT * FROM `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_scaled_dataset'] }`
WHERE local_date >= "2020-01-01" AND local_date <="2020-12-31";

INSERT INTO `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.2021`
AS
SELECT * FROM `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_scaled_dataset'] }`
WHERE local_date >= "2021-01-01" AND local_date <="2021-12-31";

INSERT INTO `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.2022`
AS
SELECT * FROM `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_scaled_dataset'] }`
WHERE local_date >= "2022-01-01" AND local_date <="2022-12-31";

INSERT INTO `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.2023`
AS
SELECT * FROM `{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_scaled_dataset'] }`
WHERE local_date >= "2023-01-01" AND local_date <="2023-12-31";