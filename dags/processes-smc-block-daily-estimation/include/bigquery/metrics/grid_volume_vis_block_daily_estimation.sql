DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
WHERE 
  run_date = '{{ ds }}' 
AND
  block='daily_estimation';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
SELECT
  *, DATE('{{ ds }}') as run_date
FROM (
  SELECT
    bqcartoeu.s2.ST_BOUNDARY(s2_id) AS polygon,
    t0.s2_id,
    t0.s2_token,
    t0.visits_score_daily_estimation,
    t0.log_visits_score_daily_estimation,
    t0.local_month,
    "daily_estimation" AS block
  FROM (
    SELECT
      s2_id,
      s2_token,
      local_month,
      SUM(visits_score_daily_estimation) AS visits_score_daily_estimation,
      LOG(nullif(SUM(visits_score_daily_estimation),
          0)) AS log_visits_score_daily_estimation,  
    FROM (
      SELECT
        visit_score AS visits_score_daily_estimation,
        DATE_TRUNC(local_date, MONTH) AS local_month,
        S2_CELLIDFROMPOINT(lat_long_visit_point,
          level => 9) AS s2_id,
        `{{ var.value.env_project }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 9) AS s2_token,
      FROM
        `{{ var.value.env_project }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}.*`
      WHERE
        (local_date < "2021-02-01" AND local_date > "2020-12-31") OR (local_date < "2020-05-01" AND local_date > "2020-03-31") )
  GROUP BY
    s2_id,
    s2_token,
    local_month ) AS t0 ) UNPIVOT ((visits,
    log_visits) FOR visit_score_step IN (
      (visits_score_daily_estimation,
      log_visits_score_daily_estimation) AS 'daily_estimation'));