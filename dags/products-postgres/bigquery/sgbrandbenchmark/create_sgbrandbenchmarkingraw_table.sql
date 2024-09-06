DECLARE this_month DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgbrand_benchmarking_table'] }}` AS

WITH

places AS(

  SELECT 
  places.pid AS fk_sgplaces,
  city_id, 
  region AS state, 
  fk_sgbrands, 
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

past_year_brand_visits AS(
  SELECT 
  fk_sgbrands,
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
  GROUP BY fk_sgbrands, tenant_type
),

current_year_brand_visits AS(
  SELECT 
  fk_sgbrands,
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
  GROUP BY fk_sgbrands, tenant_type
), 

brands_indexes AS(
  
  SELECT fk_sgbrands, 
         ROUND(100 * (SAFE_DIVIDE(brand_visits_now - brand_visits_ref, brand_visits_ref)), 2) AS brand_index
  FROM past_year_brand_visits
  INNER JOIN current_year_brand_visits
  USING(fk_sgbrands)
)

SELECT
fk_sgbrands,
this_month AS local_date,
brand_index
FROM 
brands_indexes
WHERE
  brand_index IS NOT NULL
ORDER BY brand_index ASC