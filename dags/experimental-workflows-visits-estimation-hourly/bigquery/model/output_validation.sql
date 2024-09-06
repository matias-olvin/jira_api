WITH 
theta as (
select weight from `storage-dev-olvin-com.visits_estimation.adjustments_hourly_supervised_weights` where processed_input = 'input_column'

)
predicted_values as (
  SELECT
  *except(group_hourly_visits), CAST(group_hourly_visits as FLOAT64) group_hourly_visits  ,
  (1- 	
(select weight from theta )*SQRT(ratio_visits_detected))* group_hourly_visits +  	
(select weight from theta )*SQRT(ratio_visits_detected) * poi_hourly_visits AS predicted_visits_theta
FROM
  `storage-dev-olvin-com.visits_estimation.adjustments_hourly_combined_input`
),
calc_dummy_date as (
  SELECT *, CASE 
  WHEN hour_week < 24 THEN date("2023-02-19")
  WHEN hour_week >= 24 and hour_week < 48 THEN date("2023-02-20")
  WHEN hour_week >= 48 and hour_week < 72 THEN date("2023-02-21")
  WHEN hour_week  >= 72 and hour_week < 96 THEN date("2023-02-22")
  WHEN hour_week  >= 96 and hour_week < 120 THEN date("2023-02-23")
  WHEN hour_week  >= 120 and hour_week < 144 THEN date("2023-02-24")
  WHEN hour_week  >= 144  THEN date("2023-02-25")
  END as dummy_date
  FROM predicted_values
  where gt_hourly_visits > 1/24 
),
average_correlation as (
  SELECT fk_sgplaces, dummy_date, CORR(predicted_visits_theta, gt_hourly_visits) daily_poi_correl
  from calc_dummy_date
  group by 1,2
)

select avg(IF(IS_NAN(daily_poi_correl),0,daily_poi_correl)) as overall_correl from average_correlation 

