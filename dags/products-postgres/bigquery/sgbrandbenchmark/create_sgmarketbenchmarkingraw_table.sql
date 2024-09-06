DECLARE this_month DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgmarket_benchmarking_table'] }}` AS
-- market = tenant_type in this case

WITH

places AS(

  SELECT 
  places.pid AS fk_sgplaces,
  city_id, 
  region AS state, 
  tenant_type

  FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}` places
  INNER JOIN(
    SELECT pid, tenant_type
    FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgbrandraw_table'] }}` 
  ) brands
  ON fk_sgbrands=brands.pid
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

past_year_tenant_type_visits AS(
  SELECT 
  tenant_type,
  SUM(visits_ref) AS brand_visits_ref
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
  GROUP BY tenant_type
),

current_year_tenant_type_visits AS(
  SELECT 
  tenant_type,
  SUM(visits_now) AS brand_visits_now
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
  GROUP BY tenant_type
), 

market_indices AS(
  
  SELECT tenant_type, 
         ROUND(100 * (SAFE_DIVIDE(brand_visits_now - brand_visits_ref, brand_visits_ref)), 2) AS market_index
  FROM past_year_tenant_type_visits
  INNER JOIN current_year_tenant_type_visits
  USING(tenant_type)
)

SELECT
tenant_type,
this_month AS local_date,
market_index
FROM
market_indices
WHERE
  market_index IS NOT NULL
ORDER BY market_index ASC