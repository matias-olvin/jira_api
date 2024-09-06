CALL `{{ params['activity_procedure'] }}` (
    "{{ location_key }}",
    "{{ params['project'] }}",
    "{{ demographics_table }}",
    "{{ ds }}",
    "{{ params['inference_dataset'] }}",
    "{{ inference_table }}",
    "{{ extra_columns_insert }}",
    "{{ activity_table }}",
    "{{ execution_date.subtract(months=1).strftime('%Y-%m-%d') }}",
    "{{ extra_columns_join }}",
    "{{ params['project'] }}.{{ postgres_dataset }}.{{ postgres_table }}",
    "{{ params['project'] }}.{{ monthly_table }}",
    "{{ visits_joins }}",
    "{{ params['project'] }}.{{ daily_table }}"
)