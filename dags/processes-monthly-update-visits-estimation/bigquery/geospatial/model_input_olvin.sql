CREATE TEMP TABLE poi_visits_scaled as
  SELECT
    fk_sgplaces,
    local_date,
    SUM(visits_observed) AS visits_observed
  FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['visits_aggregated_table'] }}`
    INNER JOIN (
      SELECT
        pid AS fk_sgplaces
      FROM
        `{{ var.value.prod_project }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
      where fk_sgbrands is not null
    ) USING (fk_sgplaces)

  GROUP BY
    fk_sgplaces,
    local_date
;

-- JOIN REGRESSORS
CREATE
OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['model_input_olvin_table'] }}`
-- `storage-dev-olvin-com.visits_estimation.model_input_olvin` 
PARTITION by local_date cluster by fk_sgplaces AS 

WITH 

-- timeslots we are going to use (2019 - 150 days from now) 
timestamps_table AS (
  SELECT
    DATE(timestamp_array) AS local_date,
  FROM
      UNNEST(GENERATE_TIMESTAMP_ARRAY('2019-01-01 00:00:00',
        TIMESTAMP_ADD("{{ ds }}", INTERVAL 310 DAY),
        INTERVAL 1 DAY)) AS timestamp_array 
        ),

template_local_date AS (
  SELECT
    *
  FROM
    (
      SELECT
        local_date
      FROM
        timestamps_table 
    )
    CROSS JOIN (
      SELECT
        DISTINCT fk_sgplaces,
      FROM
        poi_visits_scaled


    )
),

group_visits_collection as (
  select
    *
    from
      -- `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['group_visits_collection_table'] }}`
      `storage-prod-olvin-com.regressors.group_visits`
),
group_visits_dictionary as (
  select
    *
  from
    -- `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['group_visits_dictionary_table'] }}`
    `storage-prod-olvin-com.regressors.group_visits_dictionary`
 
),
input_group_visits as (
  select
    template_local_date.*,
    IFNULL(group_factor, 0) as group_factor
  from
    template_local_date
    inner join group_visits_dictionary using (fk_sgplaces)
    left join (
      select
        local_date,
        IFNULL(factor,0) as group_factor,
        identifier as identifier_group
      from
        group_visits_collection
    ) using (identifier_group, local_date)
  
),

holidays_collection as (
  select
    distinct local_date,
    factor,
    identifier
  from
    `{{ var.value.prod_project }}.{{ params['regressors_dataset'] }}.{{ params['holidays_collection_table'] }}`
    -- `storage-prod-olvin-com.regressors.holidays`
),
holidays_dictionary as (
  select
    *
  from
    `{{ var.value.prod_project }}.{{ params['regressors_dataset'] }}.{{ params['holidays_dictionary_table'] }}`
    -- `storage-prod-olvin-com.regressors.holidays_dictionary`
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
    IF(
      identifier_christmas = 'christmas',
      christmas_factor,
      0
    ) as christmas_factor,
  from
    template_holidays
),
weather_collection as (
  select
    *
    from
      `{{ var.value.prod_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
      -- `storage-prod-olvin-com.regressors.weather_daily`
),
weather_dictionary as (
  select
    *
  from
    `{{ var.value.prod_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_dictionary_table'] }}`
    -- `storage-prod-olvin-com.regressors.weather_dictionary`
 
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
        local_date,
        IFNULL(factor,0) as precip_intensity,
        identifier as identifier_precip_intensity
      from
        weather_collection
    ) using (identifier_precip_intensity, local_date)
    left join (
      select
        local_date,
        IFNULL(factor,0) as temperature,
        identifier as identifier_temperature
      from
        weather_collection
    ) using (identifier_temperature, local_date)
    left join (
      select
        local_date,
        IFNULL(factor,0) as temperature_res,
        identifier as identifier_temperature_res
      from
        weather_collection
    ) using (identifier_temperature_res, local_date)
),
inference_input as (
  select
    *
  except
    (y),
    ifnull(y, 0) as y,
    local_date AS ds
  from
    input_holidays
    inner join input_weather using (local_date, fk_sgplaces)
    inner join input_group_visits using (local_date, fk_sgplaces)
    left join (
      select
        visits_observed as y,
        local_date,
        fk_sgplaces,
      from
        poi_visits_scaled

    ) using (fk_sgplaces, local_date)
),
checking_no_duplicates as (
  select
    *,
    count(*) OVER (PARTITION BY fk_sgplaces, local_date) count_duplicates
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
      FORMAT("There are fk_sgplaces, local_date duplicates.")
    )
  )
