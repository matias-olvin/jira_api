CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits-estimation-metrics-dataset'] }}.{{ params['metrics-table'] }}`
COPY `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['metrics-table'] }}`;