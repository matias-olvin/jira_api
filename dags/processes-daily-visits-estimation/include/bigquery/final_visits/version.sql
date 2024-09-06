UPDATE `{{ params['bigquery-project'] }}.{{ params['postgres-rt-dataset'] }}.{{ params['version-table'] }}`
SET latest_actual_date = "{{ ti.xcom_pull(task_ids='local-date') }}"
WHERE end_date IS NULL;