CALL `{{ params['block_daily_estimation_scaling_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}",
    "{{ params['date_start'] }}",
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_block_1_dataset'] }}",
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['daily_estimation_dataset'] }}_{{ params['daily_factor_table'] }}"
)