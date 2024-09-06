DECLARE initial,final int64;
set initial = 2018;
set final = (select extract(year from current_date()));

while initial<=final do
execute immediate(concat("DROP MATERIALIZED VIEW IF EXISTS `{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['monthly_observed_visits_mv'] }}_",initial,"`"));
execute immediate(concat("CREATE MATERIALIZED VIEW `{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['monthly_observed_visits_mv'] }}_",initial,"` CLUSTER BY local_date,fk_sgplaces OPTIONS (enable_refresh = false) AS select fk_sgplaces, local_date, sum(visit_score) as visits from (select fk_sgplaces, date_trunc(local_date, month) as local_date, visit_score from `{{ var.value.env_project }}.{{ params['poi_visits_scaled_dataset'] }}.",initial,"`) group by local_date, fk_sgplaces"));
set initial = initial +1;
end while;