INSERT INTO
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGPlaceBenchmarkingRaw_table'] }}`
--  `storage-prod-olvin-com.postgres.SGPlaceBenchmarkingRaw`
  (fk_sgplaces, local_date, market, place_index, market_index, difference)
WITH

tenants_base_table AS(
  SELECT pid AS fk_sgplaces, fk_sgcenters, city_id, region AS state, tenant_type
  FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}`
--  `storage-prod-olvin-com.postgres.SGPlaceRaw`
  INNER JOIN(
    SELECT pid AS fk_sgbrands, tenant_type
    FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['sgbrandraw_table'] }}`
--    `storage-prod-olvin-com.postgres.SGBrandRaw`
  )
  USING(fk_sgbrands)
  WHERE naics_code <> 531120
),

pois_used_as_ref AS(
  SELECT fk_sgplaces
  FROM(
    SELECT fk_sgplaces, COUNT(*) as num_months
    FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
--    `storage-prod-olvin-com.postgres.SGPlaceMonthlyVisitsRaw`
    WHERE local_date >  DATE_TRUNC(DATE_SUB('{{ ds }}', INTERVAL 2 YEAR), MONTH)
      AND local_date <= DATE_TRUNC('{{ ds }}', MONTH)
    GROUP BY fk_sgplaces
  )
  WHERE num_months = 24
),

past_year_place_visits AS(
  SELECT *
  FROM tenants_base_table
  INNER JOIN(
    SELECT fk_sgplaces, SUM(visits) as visits_ref
    FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
--    `storage-prod-olvin-com.postgres.SGPlaceMonthlyVisitsRaw`
    WHERE local_date >  DATE_TRUNC(DATE_SUB('{{ ds }}', INTERVAL 2 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(DATE_SUB('{{ ds }}', INTERVAL 1 YEAR), MONTH)
    GROUP BY fk_sgplaces
  )
  USING(fk_sgplaces)
),

current_year_place_visits AS(
  SELECT *
  FROM tenants_base_table
  INNER JOIN(
    SELECT fk_sgplaces, SUM(visits) as visits_now
    FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
--    `storage-prod-olvin-com.postgres.SGPlaceMonthlyVisitsRaw`
    WHERE local_date >  DATE_TRUNC(DATE_SUB('{{ ds }}', INTERVAL 1 YEAR), MONTH)
      AND local_date <= DATE_TRUNC('{{ ds }}', MONTH)
    GROUP BY fk_sgplaces
  )
  USING(fk_sgplaces)
), 

mall_market_table AS(
  SELECT 
    fk_sgcenters, 
    num_pois_center, 
    visits_now_center, 
    visits_ref_center, 
    visits_now_center / visits_ref_center AS market_index_center
  FROM (
    SELECT 
      fk_sgcenters, 
      COUNT(*) AS num_pois_center
    FROM past_year_place_visits
    INNER JOIN pois_used_as_ref
      USING(fk_sgplaces)
    GROUP BY fk_sgcenters
  ) num_pois_data
  INNER JOIN (
    SELECT 
      fk_sgcenters, 
      SUM(mall_visits) AS visits_ref_center
    FROM `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE local_date > DATE_TRUNC(DATE_SUB('{{ ds }}', INTERVAL 2 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(DATE_SUB('{{ ds }}', INTERVAL 1 YEAR), MONTH)
    GROUP BY fk_sgcenters
  ) past_data
  USING(fk_sgcenters)
  INNER JOIN (
    SELECT 
      fk_sgcenters, 
      SUM(mall_visits) AS visits_now_center
    FROM `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE local_date > DATE_TRUNC(DATE_SUB('{{ ds }}', INTERVAL 1 YEAR), MONTH)
      AND local_date <= DATE_TRUNC('{{ ds }}', MONTH)
    GROUP BY fk_sgcenters
  ) current_data
  USING(fk_sgcenters)
),

city_market_table AS(
  SELECT *, visits_now_city/visits_ref_city AS market_index_city
  FROM(
    SELECT city_id, city, state_abbreviation
    FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['cityraw_table'] }}`
--    `storage-prod-olvin-com.postgres.CityRaw`
  )
  INNER JOIN(
    SELECT city_id, tenant_type, COUNT(*) AS num_pois_city, SUM(visits_ref) AS visits_ref_city
    FROM past_year_place_visits
    INNER JOIN pois_used_as_ref
      USING(fk_sgplaces)
    GROUP BY city_id, tenant_type
  )
  USING(city_id)
  INNER JOIN(
    SELECT city_id, tenant_type, SUM(visits_now) AS visits_now_city
    FROM current_year_place_visits
    INNER JOIN pois_used_as_ref
      USING(fk_sgplaces)
    GROUP BY city_id, tenant_type
  )
  USING(city_id, tenant_type)
),

state_market_table AS(
  SELECT *, visits_now_state/visits_ref_state AS market_index_state
  FROM(
    SELECT state, tenant_type, SUM(visits_ref) AS visits_ref_state
    FROM past_year_place_visits
    INNER JOIN pois_used_as_ref
      USING(fk_sgplaces)
    GROUP BY state, tenant_type
  )
  INNER JOIN(
    SELECT state, tenant_type, SUM(visits_now) AS visits_now_state
    FROM current_year_place_visits
    INNER JOIN pois_used_as_ref
      USING(fk_sgplaces)
    GROUP BY state, tenant_type
  )
  USING(state, tenant_type)
),

place_indexes AS(
  SELECT fk_sgplaces, IF(enough_months, SAFE_DIVIDE(visits_now, visits_ref), NULL) AS place_index
  FROM past_year_place_visits
  INNER JOIN current_year_place_visits
  USING(fk_sgplaces)
  LEFT JOIN(
    SELECT fk_sgplaces, True AS enough_months
    FROM pois_used_as_ref
  )
  USING(fk_sgplaces)
)



SELECT base_table.* EXCEPT(place_index, market_index),
       IF(closing_date IS NULL OR closing_date > DATE_TRUNC('{{ ds }}', MONTH), place_index, NULL) AS place_index,
       IF(closing_date IS NULL OR closing_date > DATE_TRUNC('{{ ds }}', MONTH), market_index, NULL) AS market_index,
       IF(closing_date IS NULL OR closing_date > DATE_TRUNC('{{ ds }}', MONTH), place_index - market_index, NULL) AS difference
FROM(
  SELECT fk_sgplaces,
        DATE_TRUNC('{{ ds }}', MONTH) AS local_date,
        place_index * 100 - 100 AS place_index,
        CASE
          WHEN num_pois_center >= 8 THEN market_index_center * 100 - 100
          WHEN num_pois_city   >= 4 THEN market_index_city   * 100 - 100
          ELSE market_index_state * 100 - 100
        END AS market_index,
        CASE
          WHEN num_pois_center >= 8 THEN mall_name
          WHEN num_pois_city   >= 4 THEN CONCAT(city, ' - ', state_abbreviation, ' / ', tenant_type)
          ELSE CONCAT(state, ' / ', tenant_type)
        END AS market
  FROM tenants_base_table
  LEFT JOIN place_indexes
    USING(fk_sgplaces)
  LEFT JOIN mall_market_table
    USING(fk_sgcenters)
  LEFT JOIN city_market_table
    USING(city_id, tenant_type)
  LEFT JOIN state_market_table
    USING(state, tenant_type)
  LEFT JOIN(
    SELECT pid AS fk_sgcenters, name AS mall_name
    FROM
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['SGCenterRaw_table'] }}`
--    `storage-prod-olvin-com.postgres.SGCenterRaw`
  )
  USING(fk_sgcenters)
) base_table
INNER JOIN
    `{{ params['project'] }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}`
--`storage-prod-olvin-com.postgres.SGPlaceRaw`
ON pid = fk_sgplaces