LOAD DATA OVERWRITE `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-supervised-output-table'] }}`
PARTITION BY local_date
FROM FILES (
  uris=[
    'gs://{{ params["model-bucket"] }}/{{ params["model-prefix"] }}/inference/rt/{{ params["stage"] }}/local_date={{ ti.xcom_pull(task_ids="local-date") }}/output/*.parquet'
  ],
  format='PARQUET')