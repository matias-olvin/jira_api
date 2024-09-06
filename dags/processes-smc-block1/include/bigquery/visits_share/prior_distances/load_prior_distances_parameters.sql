LOAD DATA OVERWRITE `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['prior_distance_parameters_table'] }}`
FROM FILES (
  format = 'CSV',
  uris = ['gs://{{ params["visits_share_bucket"] }}/{{ ds_nodash }}/prior_distance/output/parameters.csv']
)