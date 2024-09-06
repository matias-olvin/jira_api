WITH
  -- could be used
  date_range AS (
    SELECT
      result_date,
      DATE_SUB(result_date, INTERVAL 0 MONTH) AS date_begin_data,
      LAST_DAY(result_date, MONTH) AS date_end_data
    FROM (
        SELECT
          IF (
              "historical" = "historical",
              --"{{params['mode']}}" = "historical",
              PARSE_DATE('%F', "2021-01-01"),
              --PARSE_DATE('%F', "{{ execution_date.add(months=1).format('%Y-%m-01') }}"),
              DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}")
              --DATE_TRUNC(DATE_SUB( CAST( PARSE_DATE('%F', "{{ execution_date.add(months=1).format('%Y-%m-01') }}") AS DATE), INTERVAL 0 MONTH), MONTH)
              ) AS result_date
    )
  ),

-- POI features
 places_with_coordinates_and_category AS (
  SELECT
    pid AS fk_sgplaces,
    latitude,
    longitude,
    olvin_category
  FROM
    --`storage-prod-olvin-com.sg_places.20211101` c
    `{{ params['project'] }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}` c
  LEFT JOIN
    --`storage-prod-olvin-com.sg_base_tables.naics_code_subcategories` m
    `{{ params['project'] }}.{{ params['sg_base_tables_dataset'] }}.{{ params['non_sensitive_naics_codes_table'] }}` m
  USING
    (naics_code)
  ),


-- DEVICE-ID | PERSONA
 demographics_based_on_zip_id AS (
    SELECT * EXCEPT (rand)
    FROM
    (SELECT
      zip_id,
      fk_life_stages,
      fk_incomes_5 AS fk_incomes,
      row_number () over (partition by zip_id) rand
    FROM
      --`storage-prod-olvin-com.static_demographics_data.zipcode_demographics_v2` 
      `{{ params['project'] }}.{{ params['demographics_dataset'] }}.{{ params['zipcode_demographics_table'] }}`
      )
    WHERE rand = 1
    ),
  device_zipcodes AS (
  SELECT
    device_id,
    MAX(zip_id) AS zip_id
  FROM
    `{{ params['project'] }}.{{ params['device_zipcodes_dataset'] }}.*`
    --`storage-prod-olvin-com.device_zipcodes.*`  where local_date = '2022-01-01'
  GROUP BY
    device_id ),
  device_personas AS (
  SELECT
    device_id,
    fk_life_stages,
    fk_incomes
  FROM
    device_zipcodes
  LEFT JOIN
    demographics_based_on_zip_id
  USING
    (zip_id) ),


-- Visits with features
poi_visits as (
    select
      fk_sgplaces,
      visit_score,
      device_id,
      CASE
          when (local_hour < 12) and (local_hour > 3) then 'morning'
          when (local_hour >= 12) and (local_hour < 20) then 'afternoon'
          else 'evening'
      end as time_of_day,
      if(extract(DAYOFWEEK from local_date) in (1,7), 'weekend', 'weekday') as day_of_week
    from
        `{{ params['project'] }}.poi_visits_scaled.*`
    
    --where local_date >= '2022-01-01'
),

adding_visits_features as (
  SELECT
      fk_sgplaces,
      IFNULL(sum(visit_score), 0) as visits_total,
      IFNULL(sum(CASE WHEN fk_life_stages = 1 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_life_stages_1,
      IFNULL(sum(CASE WHEN fk_life_stages = 2 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_life_stages_2,
      IFNULL(sum(CASE WHEN fk_life_stages = 3 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_life_stages_3,
      IFNULL(sum(CASE WHEN fk_life_stages = 4 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_life_stages_4,
      IFNULL(sum(CASE WHEN fk_life_stages = 5 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_life_stages_5,
      IFNULL(sum(CASE WHEN fk_incomes = 1 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_incomes_1,
      IFNULL(sum(CASE WHEN fk_incomes = 2 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_incomes_2,
      IFNULL(sum(CASE WHEN fk_incomes = 3 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_incomes_3,
      IFNULL(sum(CASE WHEN fk_incomes = 4 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_incomes_4,
      IFNULL(sum(CASE WHEN fk_incomes = 5 THEN visit_score END) / nullif(sum(visit_score),0), 0) as fk_incomes_5,
      IFNULL(sum(CASE WHEN day_of_week = 'weekday' THEN visit_score END) / nullif(sum(visit_score),0), 0) as visits_weekday,
      IFNULL(sum(CASE WHEN day_of_week = 'weekend' THEN visit_score END) / nullif(sum(visit_score),0), 0) as visits_weekend,
      IFNULL(sum(CASE WHEN time_of_day = 'morning' THEN visit_score END) / nullif(sum(visit_score),0), 0) as visits_morning,
      IFNULL(sum(CASE WHEN time_of_day = 'afternoon' THEN visit_score END) / nullif(sum(visit_score), 0),0) as visits_afternoon,
      IFNULL(sum(CASE WHEN time_of_day = 'evening' THEN visit_score END) / nullif(sum(visit_score),0), 0) as visits_evening,
  FROM poi_visits
  left join device_personas using (device_id)
  group by fk_sgplaces

),

adding_poi_features as (
  select *
  from places_with_coordinates_and_category
  left join adding_visits_features using (fk_sgplaces)
),

adding_vanilla_features as (
  select * except (visits_total, visits_weekend, visits_weekday, visits_morning, visits_afternoon, visits_evening, fk_life_stages_1, fk_life_stages_2, fk_life_stages_3, fk_life_stages_4, fk_life_stages_5, fk_incomes_1, fk_incomes_2, fk_incomes_3, fk_incomes_4, fk_incomes_5),
      IFNULL(visits_total, 0) as visits_total,
      IFNULL(IF((visits_weekend = 0) and (visits_weekday = 0), 0.5, visits_weekend), 0.5) AS visits_weekend,
      IFNULL(IF((visits_weekend = 0) and (visits_weekday = 0), 0.5, visits_weekday), 0.5) AS visits_weekday,
      IFNULL(IF((visits_morning = 0) and (visits_afternoon = 0) and (visits_evening = 0) , 0.333, visits_morning), 0.333) AS visits_morning,
      IFNULL(IF((visits_morning = 0) and (visits_afternoon = 0) and (visits_evening = 0) , 0.333, visits_afternoon), 0.333) AS visits_afternoon,
      IFNULL(IF((visits_morning = 0) and (visits_afternoon = 0) and (visits_evening = 0) , 0.333, visits_evening), 0.333) AS visits_evening,
      IFNULL(IF((fk_life_stages_1 = 0) and (fk_life_stages_2 = 0) and (fk_life_stages_3 = 0) and (fk_life_stages_4 = 0) and (fk_life_stages_5 = 0)  , 0.2, fk_life_stages_1), 0.2) AS fk_life_stages_1,
      IFNULL(IF((fk_life_stages_1 = 0) and (fk_life_stages_2 = 0) and (fk_life_stages_3 = 0) and (fk_life_stages_4 = 0) and (fk_life_stages_5 = 0), 0.2, fk_life_stages_2), 0.2) AS fk_life_stages_2,
      IFNULL(IF((fk_life_stages_1 = 0) and (fk_life_stages_2 = 0) and (fk_life_stages_3 = 0) and (fk_life_stages_4 = 0) and (fk_life_stages_5 = 0), 0.2, fk_life_stages_3), 0.2) AS fk_life_stages_3,
      IFNULL(IF((fk_life_stages_1 = 0) and (fk_life_stages_2 = 0) and (fk_life_stages_3 = 0) and (fk_life_stages_4 = 0) and (fk_life_stages_5 = 0) , 0.2, fk_life_stages_4), 0.2) AS fk_life_stages_4,
      IFNULL(IF((fk_life_stages_1 = 0) and (fk_life_stages_2 = 0) and (fk_life_stages_3 = 0) and (fk_life_stages_4 = 0) and (fk_life_stages_5 = 0) , 0.2, fk_life_stages_5), 0.2) AS fk_life_stages_5,
      IFNULL(IF((fk_incomes_1 = 0) and (fk_incomes_2 = 0) and (fk_incomes_3 = 0) and (fk_incomes_4 = 0) and (fk_incomes_5 = 0) , 0.2, fk_incomes_1), 0.2) AS fk_incomes_1,
      IFNULL(IF((fk_incomes_1 = 0) and (fk_incomes_2 = 0) and (fk_incomes_3 = 0) and (fk_incomes_4 = 0) and (fk_incomes_5 = 0) , 0.2, fk_incomes_2), 0.2) AS fk_incomes_2,
      IFNULL(IF((fk_incomes_1 = 0) and (fk_incomes_2 = 0) and (fk_incomes_3 = 0) and (fk_incomes_4 = 0) and (fk_incomes_5 = 0) , 0.2, fk_incomes_3), 0.2) AS fk_incomes_3,
      IFNULL(IF((fk_incomes_1 = 0) and (fk_incomes_2 = 0) and (fk_incomes_3 = 0) and (fk_incomes_4 = 0) and (fk_incomes_5 = 0) , 0.2, fk_incomes_4), 0.2) AS fk_incomes_4,
      IFNULL(IF((fk_incomes_1 = 0) and (fk_incomes_2 = 0) and (fk_incomes_3 = 0) and (fk_incomes_4 = 0) and (fk_incomes_5 = 0) , 0.2, fk_incomes_5), 0.2) AS fk_incomes_5,


  from adding_poi_features

  )

select *
from adding_vanilla_features


