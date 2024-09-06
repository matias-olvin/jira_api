CREATE OR REPLACE TABLE  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_metadata_table'] }}` 
  cluster by group_id,fk_sgplaces as
  SELECT fk_sgplaces, timezone, group_id 
  FROM `{{ var.value.env_project }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}` 
  inner join
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['grouping_id_table'] }}`
  on pid = fk_sgplaces