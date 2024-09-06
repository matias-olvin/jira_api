DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

INSERT INTO `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-supervised-output-factors-distribution-table'] }}`
SELECT *
FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-output-factors-distribution-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
;

INSERT INTO `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-supervised-output-pois-unchanged-table'] }}`
SELECT *
FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-output-pois-unchanged-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
;