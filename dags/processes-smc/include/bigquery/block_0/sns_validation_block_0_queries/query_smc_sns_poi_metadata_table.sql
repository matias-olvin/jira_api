CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.smc_{{ params['sns_poi_metadata_table'] }}`
CLUSTER BY fk_sgplaces AS 
SELECT fk_sgplaces, LEAST(max_date, '{{ ds }}') AS max_date, GREATEST(min_date, '2019-01-01') AS min_date,
FROM `{{ params['sns_project'] }}.{{ params['accessible_by_olvin_dataset'] }}.{{ params['sns_poi_metadata_table'] }}`