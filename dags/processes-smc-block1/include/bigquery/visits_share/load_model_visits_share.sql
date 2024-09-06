CREATE OR REPLACE MODEL
`{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['visits_share_model'] }}`
OPTIONS(MODEL_TYPE = 'TENSORFLOW',
MODEL_PATH = "gs://{{ params['visits_share_bucket'] }}/{{ ds_nodash }}/classifier/model/*")