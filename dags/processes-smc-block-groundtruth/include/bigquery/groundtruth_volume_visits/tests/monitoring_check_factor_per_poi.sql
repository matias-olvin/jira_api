ASSERT (
    SELECT terminate
    FROM `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['monitoring_factor_per_poi_table'] }}`
    WHERE run_date = CURRENT_DATE()
) = False AS `terminate is True in {{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['monitoring_factor_per_poi_table'] }}`