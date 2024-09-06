DECLARE initial,final int64;
DECLARE table_name string;
set initial = 2018;
set final = (select extract(year from current_date()));

while initial<=final do
set table_name=concat("{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['daily_observed_visits_mv'] }}_",initial);
CALL BQ.REFRESH_MATERIALIZED_VIEW(table_name);

set table_name=concat("{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['monthly_observed_visits_per_device_mv'] }}_",initial);
CALL BQ.REFRESH_MATERIALIZED_VIEW(table_name);

set table_name=concat("{{ var.value.env_project }}.{{ params['materialzed_views_dataset'] }}.{{ params['monthly_observed_visits_mv'] }}_",initial);
CALL BQ.REFRESH_MATERIALIZED_VIEW(table_name);
set initial = initial +1;
end while;


-- CALL BQ.REFRESH_MATERIALIZED_VIEW('{{ var.value.env_project }}.{{ params["materialzed_views_dataset"] }}.{{ params["daily_observed_visits_mv"] }}');
-- CALL BQ.REFRESH_MATERIALIZED_VIEW('{{ var.value.env_project }}.{{ params["materialzed_views_dataset"] }}.{{ params["monthly_observed_visits_per_device_mv"] }}');
-- CALL BQ.REFRESH_MATERIALIZED_VIEW('{{ var.value.env_project }}.{{ params["materialzed_views_dataset"] }}.{{ params["monthly_observed_visits_mv"] }}');