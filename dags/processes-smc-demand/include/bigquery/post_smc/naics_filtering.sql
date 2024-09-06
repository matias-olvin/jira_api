CALL `{{ params['naics_filtering_procedure'] }}`(
    "{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}",
    "{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}"
)