DECLARE start_covid DATE DEFAULT '2020-03-01';
DECLARE end_covid DATE DEFAULT '2020-10-01';


BEGIN CREATE TEMP TABLE daily_almanac_visits as
  select
    fk_sgplaces,
    sum(visit_score_steps.daily_estimation) as daily_visits,
    SUM(visit_score_steps.original) AS daily_visits_original,
    SUM(visit_score_steps.visit_share) AS daily_visits_visit_share,
    local_date
  from 
    `{{ var.value.env_project }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}.*`
--   where local_date in ('2022-01-01', '2019-08-01')
  where local_date < start_covid or local_date > end_covid

  group by fk_sgplaces, local_date;
END;

BEGIN CREATE TEMP TABLE total_almanac_visits as
  select
    fk_sgplaces,
    sum(daily_visits) as total_visits,
    sum(daily_visits_original) as total_visits_original,
    sum(daily_visits_visit_share) as total_visits_visit_share,
  from 
    daily_almanac_visits
  group by fk_sgplaces;
END;

BEGIN CREATE TEMP TABLE range_dates as
  select * from 
  (
    select
      min(local_date) as min_date_visits,
      max(local_date) as max_date_visits,
    from 
      daily_almanac_visits
  )
  WHERE
    IF(min_date_visits < start_covid, TRUE, ERROR(FORMAT('min_date_visits < start_covid')))
    AND
    IF(max_date_visits > end_covid, TRUE, ERROR(FORMAT('(max_date_visits > end_covid')));
END;



BEGIN CREATE TEMP TABLE places_opening_dates as
select
  pid as fk_sgplaces, opening_date, closing_date
from
  `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`;
END;

BEGIN CREATE TEMP TABLE total_days_per_poi as
select 
  *,
  GREATEST(
            date_diff(LEAST(date(start_covid), date(ifnull(closing_date, '2130-01-01'))), GREATEST(date(ifnull(opening_date, '2000-01-01')), date(min_date_visits)), day),
            0
          )
  as before_covid_total,
  GREATEST(
            date_diff(LEAST(date(ifnull(closing_date, '2130-01-01')), date(max_date_visits)), GREATEST(date(end_covid), date(ifnull(opening_date, '2000-01-01'))), day),
            0
          )
  as after_covid_total,
  GREATEST(
            date_diff(LEAST(date(start_covid), date(ifnull(closing_date, '2130-01-01'))), GREATEST(date(ifnull(opening_date, '2000-01-01')), date(min_date_visits)), day),
            0
          )
  +
  GREATEST(
            date_diff(LEAST(date(ifnull(closing_date, '2130-01-01')), date(max_date_visits)), GREATEST(date(end_covid), date(ifnull(opening_date, '2000-01-01'))), day),
            0
          )
  as total_days
from
places_opening_dates, range_dates;
END;


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_ground_truth_volume_dataset'] }}.{{ params['model_input_visits_table'] }}`
AS
select fk_sgplaces,
    total_visits / nullif(total_days,0) as visits,
    total_visits_original / nullif(total_days,0) as visits_original,
    total_visits_visit_share / nullif(total_days,0) as visits_visit_share,


from total_almanac_visits
inner join total_days_per_poi using (fk_sgplaces)
