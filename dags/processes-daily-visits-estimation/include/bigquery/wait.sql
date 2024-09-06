SELECT completed
FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['monitoring-supervised-inference-dag-table'] }}`
WHERE 
  dag_id = "{{ params['trigger-dag-id'] }}"
  AND run_date = DATE("{{ ds }}")