DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

DELETE FROM `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-groups-gt-scaled-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;
INSERT INTO `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-groups-gt-scaled-table'] }}`
SELECT *
FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-groups-gt-scaled-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;

DELETE FROM `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-factors-tier-id-missing-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;
INSERT INTO `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-factors-tier-id-missing-table'] }}`
SELECT *
FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-tier-id-missing-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;

DELETE FROM `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-factors-tier-id-factors-distribution-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;
INSERT INTO `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['logging-rt-factors-tier-id-factors-distribution-table'] }}`
SELECT *
FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-factors-tier-id-factors-distribution-table'] }}`
WHERE
  run_date = "{{ ds }}"
  AND local_date = date_to_update
  AND stage = "{{ params['stage'] }}"
;