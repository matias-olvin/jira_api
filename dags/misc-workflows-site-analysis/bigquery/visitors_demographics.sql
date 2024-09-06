CALL `{{ params['demographics_site_procedure'] }}`(
    "{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_demographics_table'] }}",
    "{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_zip4_table'] }}",
    "{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['state_demographics_table'] }}"
)