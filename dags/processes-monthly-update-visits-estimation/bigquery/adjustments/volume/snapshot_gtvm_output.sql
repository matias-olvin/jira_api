DROP SNAPSHOT TABLE IF EXISTS
    `{{ var.value.env_project }}.{{ params['gtvm_dataset'] }}.{{ params['gtvm_model_output_table'] }}-{{ ds }}`;
CREATE SNAPSHOT TABLE IF NOT EXISTS
    `{{ var.value.env_project }}.{{ params['gtvm_dataset'] }}.{{ params['gtvm_model_output_table'] }}-{{ ds }}`
CLONE
    `{{ var.value.env_project }}.{{ params['gtvm_dataset'] }}.{{ params['gtvm_model_output_table'] }}`;