DECLARE this_month DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgbrandcity_benchmarking_table'] }}` AS


WITH

places AS(

  SELECT 
  places.pid AS fk_sgplaces,
  city_id, 
  region AS state, 
  fk_sgbrands

  FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}` places

),

-- only selects places that have visits during whole period 
pois_used_as_ref AS(
  SELECT fk_sgplaces
  FROM(
    SELECT fk_sgplaces, COUNT(*) as num_months
    FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
    WHERE local_date >  DATE_TRUNC(DATE_SUB(this_month, INTERVAL 2 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(this_month, MONTH)
    GROUP BY fk_sgplaces
  )
  WHERE num_months = 24
),

past_year_city_brands_visits AS(
  SELECT 
  city_id,
  fk_sgbrands,
  SUM(visits_ref) AS city_brands_visits_ref
  FROM places
  INNER JOIN(
    SELECT fk_sgplaces, SUM(visits) as visits_ref
    FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
    WHERE local_date >  DATE_TRUNC(DATE_SUB(this_month, INTERVAL 2 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(DATE_SUB(this_month, INTERVAL 1 YEAR), MONTH)
    GROUP BY fk_sgplaces
  )
  USING(fk_sgplaces)
  INNER JOIN pois_used_as_ref -- only those with enough months
  USING(fk_sgplaces)
  GROUP BY city_id, fk_sgbrands
),

current_year_city_brands_visits AS(
  SELECT 
  city_id,
  fk_sgbrands,
  SUM(visits_now) AS city_brands_visits_now
  FROM places
  INNER JOIN(
    SELECT fk_sgplaces, SUM(visits) as visits_now
    FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['SGPlaceMonthlyVisitsRaw_table'] }}`
    WHERE local_date >  DATE_TRUNC(DATE_SUB(this_month, INTERVAL 1 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(this_month, MONTH)
    GROUP BY fk_sgplaces
  )
  USING(fk_sgplaces)
  INNER JOIN pois_used_as_ref -- only those with enough months
  USING(fk_sgplaces)
  GROUP BY city_id, fk_sgbrands
), 

brands_indexes AS(
  
  SELECT 
  past_year_city_brands_visits.city_id,
  past_year_city_brands_visits.fk_sgbrands, 
  ROUND(100 * (SAFE_DIVIDE(city_brands_visits_now - city_brands_visits_ref, city_brands_visits_ref)), 2) AS chain_index
  FROM past_year_city_brands_visits
  INNER JOIN current_year_city_brands_visits
  ON past_year_city_brands_visits.city_id = current_year_city_brands_visits.city_id 
  AND past_year_city_brands_visits.fk_sgbrands = current_year_city_brands_visits.fk_sgbrands

)

SELECT
fk_sgbrands,
city_id AS fk_city,
this_month AS local_date,
chain_index 
FROM 
brands_indexes
WHERE
  chain_index IS NOT NULL
ORDER BY chain_index ASC