DECLARE start_date DATE DEFAULT '2019-01-01';
declare end_date DATE;
set end_date = DATE_ADD(LAST_DAY(CAST('{{ ds }}' AS DATE)), INTERVAL 4 MONTH);

CREATE OR REPLACE TABLE `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['visits_to_malls_daily_table'] }}` AS

WITH 

daily_from_gt AS (
    SELECT
        SGCenterRaw.name,
        v.mall,
        v.local_date,
        v.daily_visits_agg AS agg_visits_tenants,
        v.daily_visits_agg/v.num_tenants_with_gt AS avg_visits_tenants,
        Null AS busiest_tenant_visits, -- we can not retrive individual store ground truth data
        v.num_tenants_with_gt AS num_tenants, 
        CAST(ROUND(v.daily_visits_agg * (a.predicted_visitors_annually_M * 1E6 / (subquery.sum_visits_2022))) AS INT64) AS mall_visits,
        'from_sensors' AS quality
    FROM
          `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_poi_matching_dataset') }}.{{ params['malls_gt_agg_table'] }}` v
    INNER JOIN
          `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_accessible_by_olvin_dataset') }}.{{ params['visits_to_malls_model_output'] }}` a ON v.mall = a.pid
    
    INNER JOIN (
        SELECT
            mall,
            SUM(CASE WHEN EXTRACT(YEAR FROM local_date) = 2022 THEN daily_visits_agg ELSE 0 END) AS sum_visits_2022
        FROM
              `{{ params['sns-project-id'] }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_poi_matching_dataset') }}.{{ params['malls_gt_agg_table'] }}`
        GROUP BY
            mall
    ) subquery ON v.mall = subquery.mall
    
    INNER JOIN 
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgcenterraw_table'] }}` SGCenterRaw
  ON 
    v.mall=SGCenterRaw.pid

    WHERE local_date >= start_date 

ORDER BY mall, local_date)

, max_date AS (
    SELECT MAX(local_date) AS last_day_with_gt
    FROM daily_from_gt
)

-- computation for malls without ground truth starts here 

,dates AS (
  SELECT date_add(start_date, INTERVAL n DAY) AS local_date
  FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
),

avg_visits_per_visitor AS (
  SELECT local_date,
         (CASE 
           WHEN EXTRACT(MONTH FROM local_date) = 1  THEN 4.00
           WHEN EXTRACT(MONTH FROM local_date) = 2  THEN 4.15
           WHEN EXTRACT(MONTH FROM local_date) = 3  THEN 3.95
           WHEN EXTRACT(MONTH FROM local_date) = 4  THEN 4.15
           WHEN EXTRACT(MONTH FROM local_date) = 5  THEN 4.25
           WHEN EXTRACT(MONTH FROM local_date) = 6  THEN 3.75
           WHEN EXTRACT(MONTH FROM local_date) = 7  THEN 4.00
           WHEN EXTRACT(MONTH FROM local_date) = 8  THEN 3.75
           WHEN EXTRACT(MONTH FROM local_date) = 9  THEN 3.75
           WHEN EXTRACT(MONTH FROM local_date) = 10 THEN 4.25
           WHEN EXTRACT(MONTH FROM local_date) = 11 THEN 3.5
           WHEN EXTRACT(MONTH FROM local_date) = 12 THEN 3.5
          END) AS avg_visits
  FROM dates
),

get_visits as (
    
    -- For 10_or_less_active_tenants
    SELECT 
    fk_parents as mall,
    a.*,
    '10_or_less_active_tenants' AS quality 

    FROM
        `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplacedailyvisitsraw_table'] }}` a
    INNER JOIN `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplaceraw_table'] }}` b
    ON 	fk_sgplaces=pid
        
    WHERE fk_sgplaces IN (
        SELECT pid 
        FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplaceraw_table'] }}`
        WHERE fk_parents IN ( 
            SELECT pid 
            FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgcenterraw_table'] }}`
            WHERE 1 < active_places_count AND active_places_count < 11)
        )
        -- AND fk_parents NOT IN (SELECT DISTINCT(mall) FROM daily_from_gt)
    UNION ALL
    -- For over_10_active_tenants
    SELECT 
    fk_parents as mall,
    a.*,
    'over_10_active_tenants' AS quality 

    FROM
        `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplacedailyvisitsraw_table'] }}` a
    INNER JOIN `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplaceraw_table'] }}` b
    ON 	fk_sgplaces=pid
        
    WHERE fk_sgplaces IN (
        SELECT pid 
        FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplaceraw_table'] }}`
        WHERE fk_parents IN ( 
            SELECT pid 
            FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgcenterraw_table'] }}`
            WHERE active_places_count > 10)
        )
        -- AND fk_parents NOT IN (SELECT DISTINCT(mall) FROM daily_from_gt)
),


-- explode the visits
daily_visits as (
  SELECT
  DATE_ADD(local_date, INTERVAL row_number DAY) as local_date,
  CAST(visits as FLOAT64) visits,
  mall,
  quality,
  fk_sgplaces,
  row_number,

FROM
  (
    SELECT
    local_date,
    mall,
    quality,
    fk_sgplaces,
    JSON_EXTRACT_ARRAY(visits) AS visit_array, -- Get an array from the JSON string
    FROM
      get_visits
    
  )
CROSS JOIN 
  UNNEST(visit_array) AS visits -- Convert array elements to row
  WITH OFFSET AS row_number -- Get the position in the array as another column
),



mall_total AS (         
  SELECT
    fk_parents as mall,
    a.local_date,
    SUM(visits) AS agg_visits
  FROM 
    daily_visits a
  INNER JOIN 
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplaceraw_table'] }}` b
  ON
    a.fk_sgplaces = b.pid
  GROUP BY 
    mall, a.local_date
  ORDER BY
    mall, local_date              
),



intermediate_visits AS(
  SELECT 
    mall, 
    local_date, 
    COUNTIF(visits>0) as num_tenants, 
    MIN(visits) as min_visits, 
    MAX(visits) as max_visits, 
    AVG(visits) as avg_visits_tenants, 
    SUM(visits) as agg_visits_tenants,
    quality
  FROM 
    daily_visits
  LEFT JOIN 
    mall_total
  USING(mall, local_date)
  GROUP BY 
    mall, local_date, quality
),


-- Be careful as count_stores will have monthly local_date column
count_stores AS(
  SELECT 
  *, 
  1+0.7*(num_places/num_places_visits-1) AS factor_places
  FROM 
  (
    
    (
    SELECT fk_parents AS mall, COUNT(*) AS num_places
    FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplaceraw_table'] }}`
    GROUP BY fk_parents
    )

  INNER JOIN 
  
    (
    SELECT fk_parents AS mall, local_date, COUNT(*) AS num_places_visits
    FROM `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplacedailyvisitsraw_table'] }}` a -- Visits available per poi/local_date
    INNER JOIN `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgplaceraw_table'] }}` b -- Get info about the tenant
    ON b.pid= a.fk_sgplaces
    GROUP BY fk_parents, local_date
    )

  USING(mall)
                    )
),



daily_visits_to_malls AS(
  SELECT 
    SGCenterRaw.name, 
    intermediate_visits.mall, 
    intermediate_visits.local_date, 
    agg_visits_tenants, 
    intermediate_visits.avg_visits_tenants,
    max_visits as busiest_tenant_visits,
    num_tenants, 
    CAST(GREATEST(max_visits, factor_places * agg_visits_tenants /avg_visits ) AS INT64) AS mall_visits,
    quality
  FROM 
    intermediate_visits 
  INNER JOIN 
    avg_visits_per_visitor
  USING(local_date)
  INNER JOIN 
    count_stores
  ON 
    intermediate_visits.mall=count_stores.mall
  AND EXTRACT(YEAR FROM intermediate_visits.local_date) = EXTRACT(YEAR FROM count_stores.local_date)
  AND EXTRACT(MONTH FROM intermediate_visits.local_date) = EXTRACT(MONTH FROM count_stores.local_date)
  INNER JOIN 
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params['sgcenterraw_table'] }}` SGCenterRaw
  ON 
    intermediate_visits.mall=pid
  WHERE 
    num_tenants>0
)


-- FORECASTS FOR MALLS FROM GT

-- Forecast for GT MALLS 

, last_month_visits AS (
    SELECT 
        gt.mall,
        SUM(gt.mall_visits) as gt_visits
        , SUM(dvtm.mall_visits) AS estimated_visits
        , SUM(gt.mall_visits)/SUM(dvtm.mall_visits) As factor
    FROM daily_from_gt gt
    LEFT JOIN daily_visits_to_malls dvtm ON gt.mall = dvtm.mall AND gt.local_date = dvtm.local_date
    WHERE gt.local_date BETWEEN DATE_SUB('{{ ds }}', INTERVAL 2 MONTH) AND (SELECT last_day_with_gt FROM max_date)
    GROUP BY gt.mall
)


, forecasts AS (
  SELECT
  name, 
  mall, 
  local_date, 
  agg_visits_tenants, 
  avg_visits_tenants,
  busiest_tenant_visits,
  num_tenants, 
  ROUND(mall_visits*factor,1) AS mall_visits,
  quality
  FROM daily_visits_to_malls
  INNER JOIN 
  last_month_visits USING(mall)
  WHERE mall IN (SELECT DISTINCT(mall) FROM daily_from_gt)
  AND local_date> (SELECT last_day_with_gt FROM max_date)
)

, final_table AS (
 SELECT * FROM daily_from_gt
 UNION ALL 
 SELECT * FROM forecasts 
 UNION ALL 
 SELECT * FROM daily_visits_to_malls
 WHERE mall NOT IN (SELECT DISTINCT(mall) FROM daily_from_gt)
 ) 


SELECT  
  mall as fk_sgcenters, 
  name,  
  local_date, 
  agg_visits_tenants, 
  avg_visits_tenants,
  busiest_tenant_visits,
  CAST(num_tenants AS INT64) as num_tenants, 
  CAST(mall_visits AS INT64) AS mall_visits,
  FROM final_table 