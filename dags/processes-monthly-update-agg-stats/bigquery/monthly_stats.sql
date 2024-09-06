CREATE OR REPLACE TABLE `{{ params['project'] }}.{{ params['Activity_dataset'] }}.{{ params['activity_stats_table'] }}` AS

WITH
  table_activity AS (
    SELECT
      *
    FROM
      `{{ params['project'] }}.{{ params['Activity_dataset'] }}.{{ params['Place_activity_table'] }}`
    WHERE run_date =  "{{ stats_param_1(execution_date) }}"
  ),
  brands_name AS (
    SELECT 
      pid AS fk_sgbrands, 
      name AS brand_name 
    FROM 
      `{{ params['project'] }}.{{ params['places_dataset'] }}.{{ params['brands_dynamic_table'] }}`
  ),
  places AS (
    SELECT 
      pid AS fk_sgplaces,  
      fk_sgbrands 
    FROM 
      `{{ params['project'] }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}`
  ),
  final_table AS (
    SELECT *
    FROM table_activity
    LEFT JOIN places 
      USING (fk_sgplaces)
    LEFT JOIN brands_name 
      USING (fk_sgbrands)
  ),
  total AS (
    SELECT 
      COUNT(*) AS total, 
      ANY_VALUE(brand_name) AS brand_name, 
      fk_sgbrands
    FROM final_table
    GROUP BY fk_sgbrands
  ),
  active_table as (
    SELECT 
      COUNT(*) as active, 
      fk_sgbrands
    FROM final_table
    WHERE activity IN ('active', 'watch_list')
    GROUP BY fk_sgbrands
  ),
  finalisima as (
    SELECT
      brand_name,
      total.total,
      ifnull(active, 0) as active,
      fk_sgbrands
    FROM total
    LEFT JOIN active_table USING (fk_sgbrands)
  )

SELECT *
FROM finalisima
WHERE brand_name IS NOT NULL
ORDER BY brand_name