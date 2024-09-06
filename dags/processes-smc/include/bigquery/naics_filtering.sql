CALL `{{ params['naics_filtering_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}.{{ params['year_name'] }}",
    "{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}"
)
