CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`
LIKE `{{ var.value.env_project }}.{{ params['metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`;

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`
PARTITION BY local_date
CLUSTER BY s2_token, part_of_day, publisher_id
AS
SELECT 
local_date,
s2_token,
publisher_id,
part_of_day,
d_devices_visits,
n_visits,
overlap_mean,
STRUCT(
visit_score_steps_sum.original,
visit_score_steps_sum.opening,
visit_score_steps_sum.weighted,	
visit_score_steps_sum.visits_share,
visit_score_steps_sum.daily_estimation,
visit_score_steps_sum.gtvm_factor
) AS visit_score_steps_sum,
STRUCT(
factor_steps_avg.original,
factor_steps_avg.opening,
factor_steps_avg.weighted,	
factor_steps_avg.visits_share,
factor_steps_avg.daily_estimation,
factor_steps_avg.gtvm_factor
) AS factor_steps_avg,
n_visits_overlap_none,
n_visits_overlap_low,
n_visits_overlap_medium,
n_visits_overlap_high,
branded

FROM
`{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`;