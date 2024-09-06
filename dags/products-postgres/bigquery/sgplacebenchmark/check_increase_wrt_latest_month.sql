SELECT COUNTIF(local_date = "{{ ds }}") /
       COUNTIF(local_date = DATE_SUB("{{ ds }}", INTERVAL 1 MONTH)) < 1.5
FROM `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGPlaceBenchmarkingRaw_table'] }}`
