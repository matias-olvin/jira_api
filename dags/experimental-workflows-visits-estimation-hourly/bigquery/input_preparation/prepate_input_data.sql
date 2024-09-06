create
or replace table 
`storage-dev-olvin-com.visits_estimation.adjustments_hourly_combined_input` 
`{{  var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_hourly_combined_input'] }}`
as 
WITH combining_input_data as (
    SELECT
        t1.fk_sgplaces,
        t1.hour_week,
        t1.hourly_visits_scaled as gt_hourly_visits,
        t2.tier_id_olvin,
        t2.hourly_visits_scaled as group_hourly_visits,
        t3.hourly_visits_scaled as poi_hourly_visits,
        t3.ratio_visits_detected
    FROM
        `storage-dev-olvin-com.visits_estimation.adjustments_hourly_gt_visits` t1
        inner join `storage-dev-olvin-com.visits_estimation.adjustments_hourly_geo_visits_grouped` t2 using (fk_sgplaces, hour_week)
        inner join `storage-dev-olvin-com.visits_estimation.adjustments_hourly_geo_visits_poi` t3 using (fk_sgplaces, hour_week)
)
select
    *
from
    combining_input_data;

create or replace table `storage-dev-olvin-com.visits_estimation.adjustments_hourly_model_input` as

WITH
training_data as (
  SELECT (gt_hourly_visits - group_hourly_visits) as output_column, (poi_hourly_visits-group_hourly_visits)*sqrt(ratio_visits_detected) as input_column
  FROM `storage-dev-olvin-com.visits_estimation.adjustments_hourly_combined_input` 
)

SELECT * from training_data;