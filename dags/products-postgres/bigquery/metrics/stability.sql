DELETE
FROM
  `storage-prod-olvin-com.postgres_metrics.stability`
WHERE
  run_date = '{{ ds }}';
INSERT INTO
  `storage-prod-olvin-com.postgres_metrics.stability` ( 
    run_date,
    comparison_date,
    fk_sgplaces,
    ranking_stability,
    log_volume_ratio,
    trend_stability 
  )

WITH current_visits AS (
  SELECT
    fk_sgplaces,
    DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date,
    CAST(visits AS FLOAT64) visits,
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array,
    FROM
      `storage-prod-olvin-com.postgres.SGPlaceDailyVisitsRaw` )
  CROSS JOIN
    UNNEST(visit_array) AS visits
  WITH
  OFFSET
    AS row_number 
),
previous_visits AS (
  SELECT
    fk_sgplaces,
    DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date,
    CAST(visits AS FLOAT64) visits,
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      JSON_EXTRACT_ARRAY(visits) AS visit_array,
    FROM
      `storage-prod-olvin-com.postgres_staging.SGPlaceDailyVisitsRaw_prev` 
  )
  CROSS JOIN
    UNNEST(visit_array) AS visits
  WITH
  OFFSET
    AS row_number 
),
current_visits_filtered AS (
  SELECT fk_sgplaces, sum(visits) as visits, '{{ ds }}' as local_date
  FROM current_visits
  WHERE
    local_date <= DATE_SUB('{{ ds }}', INTERVAL 1 MONTH)
    AND local_date > DATE_SUB(DATE_SUB('{{ ds }}', INTERVAL 1 MONTH), INTERVAL 12 MONTH) 
  group by fk_sgplaces
),
previous_visits_filtered AS (
  SELECT fk_sgplaces, sum(visits) as visits, '{{ ds }}' as local_date
  FROM previous_visits
  WHERE
    local_date <= DATE_SUB('{{ ds }}', INTERVAL 1 MONTH)
    AND local_date > DATE_SUB(DATE_SUB('{{ ds }}', INTERVAL 1 MONTH), INTERVAL 12 MONTH) 
    group by fk_sgplaces
    
),
log_ratio_visits AS (
  SELECT
    fk_sgplaces,
    NULLIF(AVG(i.visits), 0) / NULLIF(AVG(j.visits), 0) AS log_volume_ratio
  FROM current_visits_filtered i
  INNER JOIN previous_visits_filtered j
  USING (fk_sgplaces)
  GROUP BY fk_sgplaces 
),
current_rankings AS (
  SELECT
    fk_sgplaces,
    ROW_NUMBER() OVER(ORDER BY visits DESC) AS total_ranking_current_month
  FROM current_visits_filtered 
),
previous_rankings AS (
  SELECT
    fk_sgplaces,
    ROW_NUMBER() OVER(ORDER BY visits DESC) AS total_ranking_previous_month
  FROM previous_visits_filtered 
),
joining_tables AS (
  SELECT
    *,
    ROW_NUMBER() OVER(ORDER BY RAND()) / COUNT(*) OVER() AS rd_number
  FROM current_rankings
  INNER JOIN previous_rankings
  USING (fk_sgplaces) 
),
joining_tables_sample AS (
  SELECT *
  FROM joining_tables
  WHERE rd_number < 0.01 
),
kendall_tau AS (
  SELECT
    (SUM(CASE
          WHEN ( i.total_ranking_current_month < j.total_ranking_current_month AND i.total_ranking_previous_month < j.total_ranking_previous_month ) OR ( i.total_ranking_current_month > j.total_ranking_current_month AND i.total_ranking_previous_month > j.total_ranking_previous_month ) THEN 1
        ELSE
        0
      END
        ) - SUM(CASE
          WHEN ( i.total_ranking_current_month < j.total_ranking_current_month AND i.total_ranking_previous_month > j.total_ranking_previous_month ) OR ( i.total_ranking_current_month > j.total_ranking_current_month AND i.total_ranking_previous_month < j.total_ranking_previous_month ) THEN 1
        ELSE
        0
      END
        )) / COUNT(*) AS ranking_stability
  FROM joining_tables_sample i
  CROSS JOIN joining_tables_sample j
  WHERE i.total_ranking_current_month <> j.total_ranking_current_month 
),
all_correlations AS (
  SELECT
    fk_sgplaces,
    CORR(i.visits, j.visits) trend_stability
  FROM `current_visits` i
  INNER JOIN `previous_visits` j
  USING (fk_sgplaces, local_date)
  GROUP BY fk_sgplaces 
)

SELECT
  DATE('{{ ds }}') AS run_date,
  DATE_SUB('{{ ds }}', INTERVAL 1 MONTH) AS comparison_date,
  fk_sgplaces,
  kendall_tau.ranking_stability AS ranking_stability,
  log_ratio_visits.log_volume_ratio AS log_volume_ratio,
  all_correlations.trend_stability AS trend_stability
FROM
  log_ratio_visits,
  kendall_tau
JOIN all_correlations
USING (fk_sgplaces)
