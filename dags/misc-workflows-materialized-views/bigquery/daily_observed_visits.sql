DECLARE initial,final int64;
set initial = 2018;
set final = (select extract(year from current_date()));

while initial<=final do
execute immediate(concat("DROP MATERIALIZED VIEW IF EXISTS `{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['daily_observed_visits_mv'] }}_",initial,"`"));
execute immediate(concat("CREATE MATERIALIZED VIEW `{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['daily_observed_visits_mv'] }}_",initial,"` PARTITION BY local_date CLUSTER BY fk_sgplaces OPTIONS (enable_refresh = false) AS select fk_sgplaces, local_date, sum(visit_score) as visits from `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.",initial,"` group by local_date, fk_sgplaces"));
set initial = initial +1;
end while;