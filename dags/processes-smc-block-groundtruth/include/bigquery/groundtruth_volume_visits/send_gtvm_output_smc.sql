CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['gtvm_output_table'] }}` COPY
`{{ params['sns-project-id'] }}.{{ params['accessible_by_olvin_dataset'] }}.smc_{{ params['gtvm_output_table'] }}`