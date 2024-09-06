SELECT COUNTIF(difference IS NULL) / COUNT(*) < 0.2
FROM `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGPlaceBenchmarkingRaw_table'] }}`
WHERE local_date = "{{ ds }}"