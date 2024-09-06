CALL `{{ params['inference_input_gcs_procedure'] }}`(
    "{{ var.value.env_project }}:{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_table'] }}",
    "{{ var.value.env_project }}:{{ params['visits_estimation_dataset'] }}.{{ params['model_input_olvin_table'] }}",
    "{{ var.value.env_project }}:{{ params['visits_estimation_dataset'] }}.{{ params['grouping_id_table'] }}",
    "gs://{{ params['inference_input_bucket'] }}/{{ execution_date.replace(day=1).strftime('%Y%m01') }}");