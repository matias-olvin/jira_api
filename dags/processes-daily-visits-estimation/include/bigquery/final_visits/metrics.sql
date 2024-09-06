DECLARE date_to_update DATE;
DECLARE date_to_update_minus_one DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");
SET date_to_update_minus_one = DATE_SUB(date_to_update, INTERVAL 1 DAY);

DELETE FROM `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-supervised-visits-table'] }}`
WHERE
  stage = "{{ params['stage'] }}"
  AND local_date = date_to_update
;
INSERT INTO `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-supervised-visits-table'] }}`
SELECT *
FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-supervised-visits-table'] }}`
WHERE
  stage = "{{ params['stage'] }}"
  AND local_date = date_to_update
;