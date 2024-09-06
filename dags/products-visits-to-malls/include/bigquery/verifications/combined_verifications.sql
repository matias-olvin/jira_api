-- DECLAREs
DECLARE monthly_visits_threshold DEFAULT 4500000;
DECLARE threshold DEFAULT 2.0;

-- MONTH TO MONTH CHECK MALLS TO REMOVE TEMP TABLE

CREATE OR REPLACE TEMP TABLE malls_to_remove_mtm AS
WITH
  visits_to_malls_monthly_excluding_exceptions AS (
    SELECT
        v.*
    FROM
      `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}` v
    WHERE
      local_date > '2021-09-01'
      and EXTRACT(MONTH FROM local_date) != 12
      and EXTRACT(MONTH FROM local_date) != 11
      -- Excluding April, May and June of 2020
      and NOT (EXTRACT(MONTH FROM local_date) = 4 AND EXTRACT(YEAR FROM local_date) = 2020)
      and NOT (EXTRACT(MONTH FROM local_date) = 5 AND EXTRACT(YEAR FROM local_date) = 2020)
      and NOT (EXTRACT(MONTH FROM local_date) = 6 AND EXTRACT(YEAR FROM local_date) = 2020)
      -- Excluding (local_date, month) where an opening occurred
      AND NOT EXISTS (
        SELECT 1
        FROM 
          `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` AS sub
        WHERE 
          sub.opening_date = local_date 
        AND 
          sub.fk_parents = fk_sgcenters 
        AND 
          sub.opening_date IS NOT NULL 
        AND 
          sub.fk_parents IS NOT NULL
          )
    )
  , monthly_visits_with_increase AS (
    SELECT
      fk_sgcenters,
      local_date,
      mall_visits,
      -- Compute the increase ratio
      CASE 
          WHEN fk_sgcenters = LAG(fk_sgcenters, 1) OVER (PARTITION BY fk_sgcenters ORDER BY local_date)
          THEN mall_visits / NULLIF(LAG(mall_visits, 1) OVER (PARTITION BY fk_sgcenters ORDER BY local_date), 0)
          ELSE NULL
      END AS increase_ratio
    FROM 
      visits_to_malls_monthly_excluding_exceptions 
)
SELECT DISTINCT(fk_sgcenters) AS fk_sgcenters
FROM 
  monthly_visits_with_increase -- all malls in this table are removed from the daily and monthly visits
WHERE 
  increase_ratio>threshold;

-- MONTHLY THRESHOLD CHECK MALLS TO REMOVE TEMP TABLE

CREATE OR REPLACE TEMP TABLE malls_to_remove_monthly_visits AS (
  SELECT fk_sgcenters
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}`
  WHERE
    mall_visits> monthly_visits_threshold
);

-- SPARSE MALLS CHECK MALLS TO REMOVE TEMP TABLE

CREATE OR REPLACE TEMP TABLE malls_to_remove_sparse AS
WITH _days AS (
  SELECT 
    fk_sgcenters,
    COUNT(DISTINCT(local_date)) as n_days 
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}`  
  GROUP BY 
    fk_sgcenters
)
SELECT fk_sgcenters FROM _days
WHERE n_days<100;

--------------------- CREATING MALLS TO REMOVE METRICS TABLE BEFORE REMOVING ITEMS FROM VISITS TABLES

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits_to_malls_dataset']  }}.{{ params['visits_to_malls_malls_to_remove_table'] }}_{{ dag_run.conf['dataset_postgres_template'] }}`
AS
WITH mtr_mtm AS (
    SELECT
        fk_sgcenters,
        1 AS month_to_month,
        0 AS monthly_visits_threshold,
        0 AS sparse_malls
    FROM
        malls_to_remove_mtm
),
mtr_monthly_visits AS (
    SELECT
        fk_sgcenters,
        0 AS month_to_month,
        1 AS monthly_visits_threshold,
        0 AS sparse_malls
    FROM
        malls_to_remove_monthly_visits
),
mtr_sparse_malls AS (
    SELECT
        fk_sgcenters,
        0 AS month_to_month,
        0 AS monthly_visits_threshold,
        1 AS sparse_malls
    FROM
        malls_to_remove_sparse
),
mtr_unioned AS (
SELECT *
FROM
    mtr_mtm
UNION ALL
SELECT *
FROM
    mtr_monthly_visits
UNION ALL
SELECT *
FROM
    mtr_sparse_malls
)
SELECT
  vtm_daily.*,
  mtr.month_to_month,
  mtr.monthly_visits_threshold,
  mtr.sparse_malls
FROM 
  mtr_unioned as mtr
INNER JOIN
  `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` AS vtm_daily
ON
  vtm_daily.fk_sgcenters=mtr.fk_sgcenters;

-- MONTH TO MONTH VERIFICATION

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` AS
  SELECT *
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` AS v
  LEFT JOIN 
    malls_to_remove_mtm AS m
  USING(fk_sgcenters)
  WHERE 
    m.fk_sgcenters IS NULL;

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}` AS
  SELECT *
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}` AS v
  LEFT JOIN 
    malls_to_remove_mtm AS m
  USING(fk_sgcenters)
  WHERE 
    m.fk_sgcenters IS NULL;

-- MONTHLY VISITS THRESHOLD

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` AS
  SELECT *
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` AS v
  LEFT JOIN 
    malls_to_remove_monthly_visits AS m
  USING(fk_sgcenters)
  WHERE 
    m.fk_sgcenters IS NULL;


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}` AS
  SELECT *
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}` AS v
  LEFT JOIN 
    malls_to_remove_monthly_visits AS m
  USING(fk_sgcenters)
  WHERE 
    m.fk_sgcenters IS NULL;

-- REMOVE SPARSE MALLS

CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` AS
  SELECT *
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` AS v
  LEFT JOIN 
    malls_to_remove_sparse AS m
  USING(fk_sgcenters)
  WHERE 
    m.fk_sgcenters IS NULL;


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}` AS
  SELECT *
  FROM 
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table'] }}` AS v
  LEFT JOIN 
    malls_to_remove_sparse AS m
  USING(fk_sgcenters)
  WHERE 
    m.fk_sgcenters IS NULL;
