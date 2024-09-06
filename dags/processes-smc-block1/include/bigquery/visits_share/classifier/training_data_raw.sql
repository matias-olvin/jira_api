CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['training_data_raw_table'] }}` 
PARTITION BY local_date
AS
WITH
  -- demographics based on zip_code
  demographics_0 AS (
    SELECT * EXCEPT (rd_nb)
    FROM
    (SELECT
      zip_id,
      fk_life_stages,
      fk_incomes_5 AS fk_incomes,
      row_number () over (partition by zip_id) rd_nb
    FROM
     --`storage-prod-olvin-com.static_demographics_data.zipcode_demographics_v2` 
     `{{ var.value.env_project }}.{{ params['demographics_dataset'] }}.{{ params['zipcode_demographics_table'] }}`
      )
    WHERE rd_nb = 1
    ),
-- devices_zipcode
  device_zipcodes AS (
  SELECT
    device_id,
    MAX(zip_id) AS zip_id
  FROM
    --`storage-prod-olvin-com.device_zipcodes.2021`
    `{{ var.value.env_project }}.{{ params['device_zipcodes_dataset'] }}.{{ params['visits_share_device_zipcodes_table'] }}`
  GROUP BY
    device_id ),
-- joining the last two tables to get the demographics of the devices
  demographics AS (
  SELECT
    device_id,
    fk_life_stages,
    fk_incomes
  FROM
    device_zipcodes
  LEFT JOIN
    demographics_0
  ON
    device_zipcodes.zip_id = demographics_0.zip_id ),
  -- reading the columns we need from poi_visits
  poi_visits_with_personas AS (
  SELECT
    poi_visits.fk_sgplaces,
    poi_visits.lat_long_visit_point,
    poi_visits.device_id,
    poi_visits.hour_ts,
    poi_visits.local_date,
    poi_visits.naics_code,
    visit_score.opening,
    hour_week,
    fk_incomes,
    fk_life_stages
  FROM
    --`storage-dev-olvin-com.poi_visits.*` AS poi_visits
    `{{ var.value.env_project }}.{{ params['smc_poi_visits_dataset'] }}.*` AS poi_visits
  INNER JOIN
    --`storage-prod-olvin-com.visits_share.us_filtering_places` AS places_list
    `{{ var.value.env_project }}.{{ params['smc_visits_share_dataset'] }}.{{ params['filtering_places_table'] }}` AS places_list
  ON
    poi_visits.fk_sgplaces = places_list.fk_sgplaces
  LEFT JOIN
    demographics
  ON
    demographics.device_id = poi_visits.device_id
  WHERE
    --(poi_visits.local_date >= CAST('2021-10-01' AS DATE))
    (poi_visits.local_date >= CAST("{{ params['local_date_start'] }}" AS DATE))
    --AND (poi_visits.local_date <= CAST('2022-01-01' AS DATE))
    AND (poi_visits.local_date <= CAST("{{ params['local_date_final'] }}" AS DATE))
    AND (visit_score.opening = 1)
    AND (fk_incomes IS NOT NULL)
    AND (fk_life_stages IS NOT NULL) ),

-- joining olvin_category code, centroid...
adding_categories as (
    select
        poi_visits_with_personas.* , 

        categories_match_table_old.olvin_category_old,
        categories_match_table_new.olvin_category,
        centroid 
    from poi_visits_with_personas
    -- left join historical_weather
    -- using (hour_ts, fk_csds)
    -- left join special_days 
    -- on poi_visits_with_personas.local_date = special_days.local_date
    LEFT JOIN
    (select naics_code, olvin_category as olvin_category_old from
        --`storage-prod-olvin-com.sg_base_tables.sg_categories_match` )  AS categories_match_table_old
        `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['categories_match_table'] }}` )  AS categories_match_table_old
    ON CAST(poi_visits_with_personas.naics_code as string) = categories_match_table_old.naics_code
    LEFT JOIN
    (select naics_code, olvin_category from
        --`storage-prod-olvin-com.sg_base_tables.naics_code_subcategories` )  AS categories_match_table_new
        `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}` )  AS categories_match_table_new
    ON poi_visits_with_personas.naics_code = categories_match_table_new.naics_code
    LEFT JOIN
    (select pid as fk_sgplaces, SAFE.ST_GEOGPOINT(longitude, latitude) as centroid from
        --`storage-prod-olvin-com.sg_places.20211101`
        `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
    ) AS places_table
    USING (fk_sgplaces)
),

--- weather
weather_collection as (
  select identifier, hour_ts, local_date, min(local_hour) as local_hour, avg(factor) as factor
  from
    --`storage-prod-olvin-com.regressors.us_weather`
    `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['weather_collection_table'] }}`
  WHERE
    --(local_date >= CAST('2021-10-01' AS DATE))
    (local_date >= CAST("{{ params['local_date_start'] }}" AS DATE))
    --AND (local_date <= CAST('2022-01-01' AS DATE))
    AND (local_date <= CAST("{{ params['local_date_final'] }}" AS DATE))
  group by identifier, hour_ts, local_date
),
weather_dictionary as (
  --select * from `storage-prod-olvin-com.regressors.us_weather_dictionary`
  select * from `{{ var.value.env_project }}.{{ params['smc_regressors_dataset'] }}.{{ params['weather_dictionary_table'] }}`
),
adding_weather as (
  select adding_categories.*, 
      temperature,
      IFNULL(precip_intensity, 0) as precip_intensity
  from adding_categories 
  inner join weather_dictionary using (fk_sgplaces)
  left join (select distinct hour_ts, factor as precip_intensity, identifier as identifier_precip_intensity  from weather_collection)  using (identifier_precip_intensity, hour_ts)
  left join (select distinct hour_ts, factor as temperature, identifier as identifier_temperature  from weather_collection)  using (identifier_temperature, hour_ts)
),

---------HOLIDAYSS---------
holidays_collection as (
  --select * from `storage-prod-olvin-com.regressors.us_holidays`
  select * from `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['holidays_collection_table'] }}`
),
holidays_dictionary as (
  --select * from `storage-prod-olvin-com.regressors.us_holidays_dictionary`
  select * from `{{ var.value.env_project }}.{{ params['smc_regressors_dataset'] }}.{{ params['holidays_dictionary_table'] }}`
),
adding_holidays as (
  select
    adding_weather.*,
    IFNULL(christmas_factor, 0) as christmas_factor,
    IFNULL(black_friday_factor, 0) as black_friday_factor,
    IFNULL(back_to_school_factor, 0) as back_to_school_factor,
  from adding_weather 
  inner join holidays_dictionary using (fk_sgplaces)
  left join (select distinct local_date, factor as christmas_factor  from holidays_collection where identifier = 'christmas')  using (local_date)
  left join (select distinct local_date, factor as black_friday_factor  from holidays_collection where identifier = 'black_friday')  using (local_date)
  left join (select distinct local_date, factor as back_to_school_factor  from holidays_collection where identifier = 'back_to_school')  using (local_date)
)
SELECT
  *
FROM
  adding_holidays

where olvin_category is not null