CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_table'] }}`
LIKE `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['day_stats_visits_table'] }}`