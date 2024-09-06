DECLARE this_month DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgbrandstate_benchmarking_table'] }}` AS
-- market = tenant_type in this case

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

past_year_state_brands_visits AS(
  SELECT 
  state,
  fk_sgbrands,
  SUM(visits_ref) AS state_brands_visits_ref
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
  GROUP BY state, fk_sgbrands
),

current_year_state_brands_visits AS(
  SELECT 
  state,
  fk_sgbrands,
  SUM(visits_now) AS state_brands_visits_now
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
  GROUP BY state, fk_sgbrands
), 

brands_indexes AS(
  
  SELECT 
  past_year_state_brands_visits.state,
  past_year_state_brands_visits.fk_sgbrands, 
  ROUND(100 * (SAFE_DIVIDE(state_brands_visits_now - state_brands_visits_ref, state_brands_visits_ref)), 2) AS chain_index
  FROM past_year_state_brands_visits
  INNER JOIN current_year_state_brands_visits
  ON past_year_state_brands_visits.state = current_year_state_brands_visits.state 
  AND past_year_state_brands_visits.fk_sgbrands = current_year_state_brands_visits.fk_sgbrands

)

SELECT
state as region,
fk_sgbrands,
this_month AS local_date,
chain_index 
FROM 
brands_indexes
WHERE
  chain_index IS NOT NULL
ORDER BY chain_index ASC