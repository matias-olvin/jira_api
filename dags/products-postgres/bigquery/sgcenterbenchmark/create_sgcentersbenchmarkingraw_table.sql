DECLARE this_month DATE DEFAULT DATE_TRUNC(CURRENT_DATE(), MONTH);

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgcenters_benchmarking_table'] }}` AS

WITH

-- only selects places that have visits during whole period 
centers_used_as_ref AS(
  SELECT fk_sgcenters
  FROM(
    SELECT fk_sgcenters, COUNT(*) as num_months
    FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE local_date >  DATE_TRUNC(DATE_SUB(this_month, INTERVAL 2 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(this_month, MONTH)
    GROUP BY fk_sgcenters
  )
  WHERE num_months = 24
),

past_year_centers_visits AS(
  SELECT 
  fk_sgcenters
  , SUM(mall_visits) as visits_ref
    FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE local_date >  DATE_TRUNC(DATE_SUB(this_month, INTERVAL 2 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(DATE_SUB(this_month, INTERVAL 1 YEAR), MONTH)
      AND fk_sgcenters IN (SELECT * FROM centers_used_as_ref)
    GROUP BY fk_sgcenters

),

current_year_centers_visits AS(
  SELECT 
  fk_sgcenters
  , SUM(mall_visits) as visits_now
    FROM `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['visits_to_malls_monthly_table'] }}`
    WHERE local_date >  DATE_TRUNC(DATE_SUB(this_month, INTERVAL 1 YEAR), MONTH)
      AND local_date <= DATE_TRUNC(this_month, MONTH)
      AND fk_sgcenters IN (SELECT * FROM centers_used_as_ref)
    GROUP BY fk_sgcenters
), 


centers_idices AS(
  
  SELECT 
  fk_sgcenters, 
  ROUND(100 * (SAFE_DIVIDE(visits_now - visits_ref, visits_ref)), 2) AS center_index
  FROM past_year_centers_visits
  INNER JOIN current_year_centers_visits
  USING(fk_sgcenters)

)

SELECT
fk_sgcenters, 
this_month AS local_date,
center_index 
FROM 
centers_idices
WHERE
  center_index IS NOT NULL
ORDER BY center_index ASC