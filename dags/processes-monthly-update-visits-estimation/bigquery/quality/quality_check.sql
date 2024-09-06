BEGIN
  CREATE temp TABLE places_table AS

  SELECT
    pid AS fk_sgplaces,
    name,
    street_address,
    naics_code,
    essential_retail
  FROM
    `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
    -- `storage-prod-olvin-com.postgres_final.SGPlaceRaw`
  LEFT JOIN
    (select naics_code, essential_retail from
      -- `storage-prod-olvin-com.sg_base_tables.naics_code_subcategories`
      `{{ var.value.prod_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
    )
  USING
    (naics_code) 
  where fk_sgbrands is not null
    
;end;

BEGIN
  CREATE temp TABLE monthly_visits AS

    SELECT
      DATE(TIMESTAMP_TRUNC(local_date, MONTH)) AS local_month,
      SUM(visits) AS visits,
      fk_sgplaces,
      FALSE as previous
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_table'] }}`
        -- `storage-dev-olvin-com.visits_estimation.ground_truth_output` 
    GROUP BY local_month, fk_sgplaces

;end;


BEGIN
  CREATE temp TABLE monthly_visits_previous AS
    SELECT
      DATE(TIMESTAMP_TRUNC(local_date, MONTH)) AS local_month,
      SUM(visits) AS visits,
      fk_sgplaces,
      TRUE as previous
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_previous_table'] }}`
        -- `storage-dev-olvin-com.visits_estimation.quality_output_previous` 
    GROUP BY local_month, fk_sgplaces

;end;

BEGIN
  CREATE temp TABLE monthly_visits_all as

  select *
  from monthly_visits

  union all

  select *
  from monthly_visits_previous

;end;




---------------- TESTS ----------------

-- TEST 3 --
  -- more than 15000 daily on average and not a mall
  BEGIN
  CREATE temp TABLE temp_too_many_visits AS

  SELECT
    fk_sgplaces,
    previous
  FROM
    (select fk_sgplaces, previous, naics_code, name, avg(visits) as monthly_avg_visits
    from 
    monthly_visits_all
    inner join places_table using (fk_sgplaces)
    group by 1,2,3,4)
  WHERE
    (monthly_avg_visits > 30 * 15000)
    AND (naics_code <> 531120) 
    AND (naics_code <> 452311)
    AND NOT CONTAINS_SUBSTR(name,
      'Shopping Mall') 

  ;END
  ;
-- TEST 4 ---
  -- less than 15 daily on average
BEGIN
CREATE temp TABLE temp_too_little_visits AS

  SELECT
    fk_sgplaces,
    previous
  FROM
    (select fk_sgplaces, previous, naics_code, name, avg(visits) as monthly_avg_visits
    from 
    monthly_visits_all
      inner join places_table using (fk_sgplaces)
    group by 1,2,3,4)
  WHERE
    monthly_avg_visits < 30 * 15
  ;END;

--- TEST 5 ---
  -- More visits in covid than in real
BEGIN
  CREATE temp TABLE temp_too_many_in_extreme_covid AS
WITH
  too_many_in_extreme_covid AS (
  SELECT
    fk_sgplaces,
    previous
  FROM
    (select fk_sgplaces,
            previous,
            essential_retail,
            avg(if( local_month ='2020-04-01', visits, null) ) as visits_estimated_4_2020,
            avg(if( local_month ='2020-05-01', visits, null) ) as visits_estimated_5_2020,
            avg(if( local_month ='2020-01-01', visits, null) ) as visits_estimated_1_2020,
            avg(if( local_month ='2021-06-01', visits, null) ) as visits_estimated_6_2021,
    from 
    monthly_visits_all
      inner join places_table using (fk_sgplaces)
    group by 1,2,3)
  WHERE
    ((visits_estimated_4_2020 > 1.5 * visits_estimated_1_2020)
         OR (visits_estimated_5_2020 > 1.5 * visits_estimated_1_2020)
         OR (visits_estimated_4_2020 > 1.5 * visits_estimated_6_2021)
         OR (visits_estimated_5_2020 > 1.5 * visits_estimated_6_2021))
    AND essential_retail = False
    )
SELECT
  *
FROM
  too_many_in_extreme_covid

  ;END;


--- TEST 6 ---
  -- More visits in covid than in real
BEGIN
  CREATE temp TABLE temp_too_many_in_mid_covid AS
WITH
  too_many_in_mid_covid AS (
  SELECT
    fk_sgplaces,
    previous
  FROM
    (select fk_sgplaces,
            previous,
            essential_retail,
            avg(if( local_month ='2020-01-01', visits, null) ) as visits_estimated_1_2020,
            avg(if( local_month ='2020-09-01', visits, null) ) as visits_estimated_9_2020,
            avg(if( local_month ='2021-01-01', visits, null) ) as visits_estimated_1_2021,
            avg(if( local_month ='2021-06-01', visits, null) ) as visits_estimated_6_2021,
    from 
    monthly_visits_all
    inner join places_table using (fk_sgplaces)
    group by 1,2,3)
  WHERE
    ((visits_estimated_9_2020 > 1.8 * visits_estimated_1_2020)
    OR (visits_estimated_1_2021 > 2 * visits_estimated_1_2020)
    OR (visits_estimated_9_2020 > 1.8 * visits_estimated_6_2021)
    OR (visits_estimated_1_2021 > 2 * visits_estimated_6_2021))
    AND essential_retail = False
    )
SELECT
  *
FROM
  too_many_in_mid_covid  
; END ;
--- TEST 7 ---
BEGIN
  CREATE temp TABLE temp_visits_remain_zero AS

  SELECT
    fk_sgplaces,
    previous
  FROM
    (select fk_sgplaces,
            previous,
            essential_retail,
            avg(if( local_month ='2020-09-01', visits, null) ) as visits_estimated_9_2020,
            avg(if( local_month ='2021-01-01', visits, null) ) as visits_estimated_1_2021,
    from 
    monthly_visits_all
    inner join places_table using (fk_sgplaces)

    group by 1,2,3)
  WHERE
    (visits_estimated_9_2020 < 30 * 5)
    OR (visits_estimated_1_2021 < 30 * 5) 
; END ;


--- TEST 8 and 9 ---
-- future trend goes to 0 (1/3 of last month)
BEGIN
  CREATE temp TABLE temp_future_visits_go2zero AS
  SELECT
    fk_sgplaces,
    previous

  FROM
  (select fk_sgplaces,
            previous,
            avg(if( local_month =DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 0 MONTH), visits, null) ) as visits_estimated_last_month,
            avg(if( local_month =DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 3 MONTH), visits, null) ) as visits_estimated_long_future,
    from 
    monthly_visits_all
    group by 1,2)
  WHERE
  (visits_estimated_long_future < 
      CASE 
        WHEN EXTRACT(MONTH FROM DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 3 MONTH)) = 12 THEN 0.8
        WHEN EXTRACT(MONTH FROM DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 0 MONTH)) = 12 THEN 0.4
        ELSE 0.5
      END    
    * visits_estimated_last_month)  

  ;END;

BEGIN
  CREATE temp TABLE temp_future_visits_go_high AS
  SELECT
    fk_sgplaces,
    previous
  FROM
  (select fk_sgplaces,
            previous,
            avg(if( local_month =DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 0 MONTH), visits, null) ) as visits_estimated_last_month,
            avg(if( local_month =DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 3 MONTH), visits, null) ) as visits_estimated_long_future,
    from 
    monthly_visits_all
    group by 1,2)
  WHERE
    (visits_estimated_long_future > 
      CASE 
        WHEN EXTRACT(MONTH FROM DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 3 MONTH)) = 12 THEN 3.0
        WHEN EXTRACT(MONTH FROM DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 3 MONTH)) = 11 THEN 2.5
        ELSE 2.2
      END
     * visits_estimated_last_month)  
;END;

--- TEST 10 ---
-- Not allowing doubling visits from month to month (before tweak) from 2021 onwards
BEGIN
  CREATE temp TABLE temp_double_increase_month_to_month AS
WITH

  monthly_visits_increase AS (
  SELECT
    visits / NULLIF((LAG(visits, 1) OVER (PARTITION BY fk_sgplaces ORDER BY fk_sgplaces, previous, local_month)),
      0) AS increase,
    fk_sgplaces,
    -- this is because it will jump from poi to poi
     LAG(fk_sgplaces, 1) OVER (PARTITION BY fk_sgplaces ORDER BY fk_sgplaces, previous, local_month) AS fk_sgplaces_next,
     LAG(previous, 1) OVER (PARTITION BY fk_sgplaces ORDER BY fk_sgplaces, previous, local_month) AS previous_next,
     previous,
     local_month
      
  FROM (
    SELECT
      *
    FROM
      monthly_visits_all
    WHERE
      local_month > '2021-09-01'
    
      )
    ),
  double_increase_month_to_month AS (
  SELECT
    fk_sgplaces,
    previous,
    local_month
  FROM (
    SELECT
      DISTINCT fk_sgplaces, previous, local_month
    FROM
      monthly_visits_increase
    WHERE
      increase > 1.85
      and EXTRACT(MONTH FROM local_month) != 12 -- overlooks the increase in Christmas season
      and EXTRACT(MONTH FROM local_month) != 11 -- overlooks the increase in Christmas/Black Friday season
      AND NOT (
        
        EXTRACT(MONTH FROM local_month) = 10 -- overlooks the increase from September to October in specific brand
        AND fk_sgplaces IN (
                            SELECT 
                            pid
                            FROM  `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                            WHERE fk_sgbrands='SG_BRAND_f0522c7c6ecb0594fd2a2e2f693eb3d9'
                            )

      )
      AND NOT (
        
        EXTRACT(MONTH FROM local_month) = 6 -- overlooks the increase from May to June 2023 in the city of Allen
        AND EXTRACT(YEAR FROM local_month) = 2023
        AND fk_sgplaces IN (
                            SELECT 
                            pid
                            FROM  `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                            WHERE city='Allen')
      )
      and fk_sgplaces = fk_sgplaces_next
      and previous = previous_next
      )
      )
SELECT
  *
FROM
  double_increase_month_to_month;
END
  ;

--- TEST 10 ---
-- Not allowing doubling visits from month to month (before tweak) from 2021 onwards
BEGIN
  CREATE temp TABLE temp_decrease_month_to_month AS
WITH

  monthly_visits_increase AS (
  SELECT
    visits / NULLIF((LAG(visits, 1) OVER (PARTITION BY fk_sgplaces ORDER BY fk_sgplaces, previous, local_month)),
      0) AS increase,
    fk_sgplaces,
    -- this is because it will jump from poi to poi
     LAG(fk_sgplaces, 1) OVER (PARTITION BY fk_sgplaces ORDER BY fk_sgplaces, previous, local_month) AS fk_sgplaces_next,
     LAG(previous, 1) OVER (PARTITION BY fk_sgplaces ORDER BY fk_sgplaces, previous, local_month) AS previous_next,
     previous,
     local_month
      
  FROM (
    SELECT
      *
    FROM
      monthly_visits_all
    WHERE
      local_month > '2021-09-01'
      and local_month <= DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 3 MONTH)
      )
    ),
  double_increase_month_to_month AS (
  SELECT
    fk_sgplaces,
    previous,
    local_month
  FROM (
    SELECT
      DISTINCT fk_sgplaces, previous, local_month
    FROM
      monthly_visits_increase
    WHERE
      increase < 0.55
      AND EXTRACT(MONTH FROM local_month) != 1 -- overlooks the decrease from December to January
      AND EXTRACT(MONTH FROM local_month) != 9 -- overlooks the decrease from August to September
      AND NOT (
        
        EXTRACT(MONTH FROM local_month) = 11 -- overlooks the decrease from October to November in specific brand
        AND fk_sgplaces IN (
                            SELECT 
                            pid
                            FROM  `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                            WHERE fk_sgbrands='SG_BRAND_f0522c7c6ecb0594fd2a2e2f693eb3d9'
                            )

      )
      AND NOT (
        
        EXTRACT(MONTH FROM local_month) = 5 -- overlooks the decrease from April to May in 2023 in the city of Allen
        AND EXTRACT(YEAR FROM local_month) = 2023
        AND fk_sgplaces IN (
                            SELECT 
                            pid
                            FROM  `{{ var.value.prod_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                            WHERE city='Allen')
      )

      
      and fk_sgplaces = fk_sgplaces_next
      and previous = previous_next
      )
  ) 
      
SELECT
  *
FROM
  double_increase_month_to_month;
END
  ;

--- TEST 11 ---
-- Monthly
BEGIN 
  CREATE TEMP TABLE temp_constant_trend_decrease_or_increase as
  SELECT
    fk_sgplaces,
    previous,
    CASE 
        WHEN ( visits_estimated_long_future > 2.75 *  visits_estimated_8_2019 ) THEN 'constant_positive_trend'
        ELSE 'constant_negative_trend'
    END AS cause

  FROM
  (select fk_sgplaces,
            previous,
            avg(if( local_month ='2019-09-01', visits, null) ) as visits_estimated_8_2019,
            avg(if( local_month = DATE_ADD( CAST(DATE_TRUNC(CAST("{{ ds }}" AS DATE) , MONTH) AS DATE), INTERVAL 3 MONTH), visits, null) ) as visits_estimated_long_future,
    from 
    monthly_visits_all
    group by 1,2)
      where ( visits_estimated_long_future > 2.75 *  visits_estimated_8_2019 ) or ( visits_estimated_long_future < 0.25 *  visits_estimated_8_2019 )

;END;


-- FINAL LIST --
-- list of spurius places
CREATE OR REPLACE TABLE
  -- `storage-dev-olvin-com.visits_estimation.spurious_places`
  `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['spurious_places_table'] }}`
as
WITH
spurius_places as (
    select fk_sgplaces, previous, 'too_many_visits' as cause from temp_too_many_visits
    union all
    select fk_sgplaces, previous, 'too_little_visits' as cause from temp_too_little_visits
    union all
    select fk_sgplaces, previous, 'too_many_in_extreme_covid' as cause from temp_too_many_in_extreme_covid
    union all
    select fk_sgplaces, previous, 'too_many_in_mid_covid' as cause from temp_too_many_in_mid_covid
    union all
    select fk_sgplaces, previous, 'visits_remain_zero_after_covid' as cause from temp_visits_remain_zero
    union all
    select fk_sgplaces, previous, 'future_visits_go_zero' from temp_future_visits_go2zero
    union all
    select fk_sgplaces, previous, 'future_visits_go_high' from temp_future_visits_go_high
    union all
    select fk_sgplaces, previous, 'monthly_increase' as cause from temp_double_increase_month_to_month
    union all
    select fk_sgplaces, previous, 'monthly_decrease' as cause from temp_decrease_month_to_month
    union all
    select fk_sgplaces, previous, cause  from temp_constant_trend_decrease_or_increase
)
select fk_sgplaces,previous,cause
from spurius_places
