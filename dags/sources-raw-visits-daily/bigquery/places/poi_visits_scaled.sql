CALL `{{ params['project'] }}.{{ params['post_block_1_scaling_procedure'] }}`(
     "{{ params['project'] }}.{{ params['poi_visits_scaled_dataset'] }}.{{execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y') }}",
     "{{ params['project'] }}.{{ params['poi_visits_staging_dataset'] }}.{{ params['block_1_output_table'] }}_{{ ds }}",
     "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}",
     "{{ execution_date.subtract(days=(var.value.latency_days_visits|int-1)).strftime('%Y-%m-%d') }}",
     "{{ params['project'] }}.{{ params['daily_estimation_dataset'] }}.{{ params['daily_factor_table'] }}",
     "{{ params['project'] }}.{{ params['ground_truth_volume_model_dataset'] }}.{{ params['ground_truth_model_factor_per_poi_table'] }}"
)