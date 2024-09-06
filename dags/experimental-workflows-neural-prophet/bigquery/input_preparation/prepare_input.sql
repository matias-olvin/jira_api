
-- JOIN REGRESSORS
CREATE
OR REPLACE TABLE `{{ params['env_project'] }}.{{ visits_estimation_model_dev_dataset }}.{{ olvin_input: model_input_olvin
 }}_{{ year_name }}`;
-- `storage-dev-olvin-com.visits_estimation.model_input_olvin_2023` 
PARTITION by local_date cluster by fk_sgplaces AS 
WITH 
##  FILTERS OUT THE POIS WE WANT FOR SAMPLE - REMOVED IN PRODUCTION 
get_pois as (
select distinct fk_sgplaces
from
(select *
from `sns-vendor-olvin-poc.poi_matching.prod_matching_extended` 
where date_range_hourly = 'Highest' and
hours_per_day_metric = 'Highest' and
accuracy = 'Highest' and
dates_density in ('High', 'Highest') and
consistency_batch_daily_feed = 'Highest'
) t1

inner join (select pid as fk_sgplaces, name, timezone, fk_sgbrands from `storage-prod-olvin-com.sg_places.places_dynamic`
  where fk_sgbrands in ('SG_BRAND_5deb800ce9500e72e355137ab8b48fb6', 'SG_BRAND_c50f7c7cdd1a67e679e05e8ab3f83478', 'SG_BRAND_95a1de80f566759aafdc2a957a76de5a') 
) using (fk_sgplaces)

),
clean_duplicates_bug as (
  SELECT * EXCEPT(max_visits) FROM (select *, max(visit_score) OVER (partition by fk_sgplaces, hour_ts) max_visits  from `storage-dev-olvin-com.poi_visits_scaled.all`
) where visit_score = max_visits

),
## TO HERE
poi_visits_scaled AS (
  SELECT
    fk_sgplaces,
    fk_sgbrands,
    local_date,
    hour_ts,
    SUM(visit_score) AS visits_observed
  FROM
    clean_duplicates_bug
    INNER JOIN (
      SELECT
        pid AS fk_sgplaces,
        timezone
      FROM
        `storage-dev-olvin-com.sg_places.places_dynamic` where pid in (select fk_sgplaces from get_pois)
    ) USING (fk_sgplaces)
  WHERE
    fk_sgbrands is not null and local_date >= start_date and local_date < end_date --and local_date = '2021-03-25'
  GROUP BY
    fk_sgplaces,
    fk_sgbrands,
    local_date,
    hour_ts
),
-- timeslots we are going to use (2019 - 150 days from now) 
timestamps_table AS (
  SELECT
    UNIX_SECONDS(timestamp_array) AS hour_ts,
  FROM
      UNNEST(GENERATE_TIMESTAMP_ARRAY('2019-01-01 00:00:00',
        TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 150 DAY),
        INTERVAL 1 HOUR)) AS timestamp_array 
        ),

template AS (
  SELECT
    *
  FROM
    (
      SELECT
        hour_ts
      FROM
        timestamps_table WHERE DATE(TIMESTAMP_SECONDS(hour_ts))>=start_date AND 
        DATE(TIMESTAMP_SECONDS(hour_ts))< end_date
    )
    CROSS JOIN (
      SELECT
        DISTINCT pid as fk_sgplaces,
      FROM
        `storage-dev-olvin-com.sg_places.places_dynamic` where pid in (select fk_sgplaces from get_pois)
    )
),

places_table as (
 select pid as fk_sgplaces, timezone from `storage-dev-olvin-com.sg_places.places_dynamic` where pid in (select distinct fk_sgplaces from get_pois)
  ),

template_local_date as (
  select
    *,
    DATE(TIMESTAMP_SECONDS(hour_ts), timezone) AS local_date
  from
    template
     inner join (select 
                    fk_sgplaces,
                    timezone 
                from places_table )
    USING (
        fk_sgplaces
    )
),
holidays_collection as (
  select
    distinct local_date,
    factor,
    identifier
  from
    `storage-prod-olvin-com.regressors.holidays`
),
holidays_dictionary as (
  select
    *
  from
    `storage-prod-olvin-com.regressors.holidays_dictionary`
),
template_holidays as (
  select
    template_local_date.*,
    holidays_dictionary.*
  EXCEPT
    (fk_sgplaces),
    IFNULL(christmas_factor, 0) as christmas_factor
  from
    template_local_date
    inner join holidays_dictionary using (fk_sgplaces)
    left join (
      select
        local_date,
        factor as christmas_factor
      from
        holidays_collection
      where
        identifier = 'christmas'
    ) using (local_date)
),
input_holidays as (
  select
    template_holidays.fk_sgplaces,
    template_holidays.local_date,
    template_holidays.hour_ts,
    IF(
      identifier_christmas = 'true',
      christmas_factor,
      0
    ) as christmas_factor,
  from
    template_holidays
),
weather_collection as (
  select
    distinct hour_ts,
    factor,
    identifier
  FROM
    (
      select
        identifier,
        hour_ts,
        local_date,
        min(local_hour) as local_hour,
        avg(factor) as factor
      from
        `storage-prod-olvin-com.regressors.weather`
      where
        local_date < DATE_ADD(end_date, INTERVAL 2 DAY) # add a few more days to account for difference in timezones
      group by
        identifier,
        hour_ts,
        local_date
    )
),
weather_dictionary as (
  select
    *
  from
    `storage-prod-olvin-com.regressors.weather_dictionary`
),
input_weather as (
  select
    template_local_date.*,
    IFNULL(temperature_res, 0) as temperature_res,
    IFNULL(temperature,0) as temperature,
    IFNULL(precip_intensity, 0) as precip_intensity
  from
    template_local_date
    inner join weather_dictionary using (fk_sgplaces)
    left join (
      select
        hour_ts,
        IFNULL(factor,0) as precip_intensity,
        identifier as identifier_precip_intensity
      from
        weather_collection
    ) using (identifier_precip_intensity, hour_ts)
    left join (
      select
        hour_ts,
        IFNULL(factor,0) as temperature,
        identifier as identifier_temperature
      from
        weather_collection
    ) using (identifier_temperature, hour_ts)
    left join (
      select
        hour_ts,
        IFNULL(factor,0) as temperature_res,
        identifier as identifier_temperature_res
      from
        weather_collection
    ) using (identifier_temperature_res, hour_ts)
),
inference_input as (
  select
    *
  except
    (y),
    ifnull(y, 0) as y,
    TIMESTAMP_SECONDS(hour_ts) AS ds
  from
    input_holidays
    inner join input_weather using (local_date, fk_sgplaces, hour_ts)
    left join (
      select
        visits_observed as y,
        hour_ts,
        fk_sgplaces,
      from
        poi_visits_scaled
    ) using (fk_sgplaces, hour_ts)
),
checking_no_duplicates as (
  select
    *,
    count(*) OVER (PARTITION BY fk_sgplaces, hour_ts) count_duplicates
  from
    inference_input
)

-- select * from checking_no_duplicates
select
  *
except
  (count_duplicates)
from
  checking_no_duplicates
WHERE
  IF(
    count_duplicates = 1,
    TRUE,
    ERROR(
      FORMAT("There are fk_sgplaces, hour_ts duplicates.")
    )
  )
