DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
WHERE
  run_date = '{{ ds }}'
AND
  block='groundtruth';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
SELECT
  *, DATE('{{ ds }}') as run_date
FROM (
  SELECT
    bqcartoeu.s2.ST_BOUNDARY(s2_id) AS polygon,
    t0.s2_id,
    t0.s2_token,
    t0.visits_score_gtvm,
    t0.log_visits_score_gtvm,
    t0.local_month,
    "groundtruth" AS block
  FROM (
    SELECT
      s2_id,
      s2_token,
      local_month,
      SUM(visits_score_gtvm) AS visits_score_gtvm,
      LOG(nullif(SUM(visits_score_gtvm),
          0)) AS log_visits_score_gtvm,  
    FROM (
      SELECT
        visit_score AS visits_score_gtvm,
        DATE_TRUNC(local_date, MONTH) AS local_month,
        S2_CELLIDFROMPOINT(lat_long_visit_point,
          level => 9) AS s2_id,
        `{{ var.value.env_project }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 9) AS s2_token,
      FROM
        `{{ var.value.env_project }}.{{ params['smc_poi_visits_scaled_dataset'] }}.*`
      WHERE
        (local_date < "2021-02-01" AND local_date > "2020-12-31") OR (local_date < "2020-05-01" AND local_date > "2020-03-31") )
  GROUP BY
    s2_id,
    s2_token,
    local_month ) AS t0 ) UNPIVOT ((visits,
    log_visits) FOR visit_score_step IN (
      (visits_score_gtvm,
      log_visits_score_gtvm) AS 'GTVM/Final'));