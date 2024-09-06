CALL `{{ params['block_groundtruth_scaling_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_scaled_dataset'] }}",
    "{{ params['date_start'] }}",
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}",
    "{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}"
)