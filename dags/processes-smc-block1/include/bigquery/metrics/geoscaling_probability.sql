-- NOT IN USE
DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['geoscaling_probability_table'] }}`
WHERE run_date = '{{ ds }}';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['geoscaling_probability_table'] }}`
with metric as
(
SELECT 
       local_date,
       COUNT(*) AS num_visits,
       COUNTIF(local_p < 0.05) / COUNT(*) AS proportion_of_0,
       COUNTIF(local_p > 0.95) / COUNT(*) AS proportion_of_1,
       COUNTIF(local_p IS NULL) / COUNT(*) AS proportion_of_nulls,
       AVG(local_p) AS mean,
       approx_quantiles(local_p, 100)[offset(05)] AS percentile_05,
       approx_quantiles(local_p, 100)[offset(10)] AS percentile_10,
       approx_quantiles(local_p, 100)[offset(25)] AS percentile_25,
       approx_quantiles(local_p, 100)[offset(50)] AS percentile_50,
       approx_quantiles(local_p, 100)[offset(75)] AS percentile_75,
       approx_quantiles(local_p, 100)[offset(90)] AS percentile_90,
       approx_quantiles(local_p, 100)[offset(95)] AS percentile_95
FROM(
       SELECT local_date, 
              visit_score_steps.geoscaling_probability/visit_score_steps.visit_share AS local_p
       FROM `{{ var.value.env_project }}.{{ params['poi_visits_block_1_dataset'] }}.*` 
       WHERE visit_score_steps.visit_share > 0 
)
GROUP BY local_date
ORDER BY local_date
)
SELECT *, DATE('{{ ds }}') as run_date from metric;