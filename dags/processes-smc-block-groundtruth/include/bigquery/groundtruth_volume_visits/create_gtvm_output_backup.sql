CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['gtvm_output_backup_table'] }}`

COPY
  `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['gtvm_output_table'] }}`