ASSERT (
  SELECT
    COUNT(*)
  FROM
    `{{ var.value.env_project }}.{{ params['smc_regressors_dataset'] }}.{{ params['weather_dictionary_table'] }}` t1
) = (
  SELECT
    COUNT(*)
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
)