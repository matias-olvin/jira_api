DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
WHERE
  run_date = '{{ ds }}'
AND
  block='0';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
SELECT
  *, DATE('{{ ds }}') as run_date
FROM (
  SELECT
    bqcartoeu.s2.ST_BOUNDARY(s2_id) AS polygon,
    t0.s2_id,
    t0.s2_token,
    t0.visits_opening,
    t0.log_visits_opening,
    t0.visits_original,
    t0.log_visits_original,
    t0.local_month,
    "0" AS block
  FROM (
    SELECT
      s2_id,
      s2_token,
      local_month,
      SUM(visits_opening) AS visits_opening,
      LOG(nullif(SUM(visits_opening),
          0)) AS log_visits_opening,
      SUM(visits_original) AS visits_original,
      LOG(nullif(SUM(visits_original),
          0)) AS log_visits_original
    FROM (
      SELECT
        visit_score.opening AS visits_opening,
        visit_score.original AS visits_original,
        DATE_TRUNC(local_date, MONTH) AS local_month,
        `{{ var.value.env_project }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 9) AS s2_token,
        S2_CELLIDFROMPOINT(lat_long_visit_point,
          level => 9) AS s2_id,
      FROM
        `{{ var.value.env_project }}.{{ params['smc_poi_visits_dataset'] }}.*`
      WHERE
        (local_date < "2021-02-01" AND local_date > "2020-12-31") OR (local_date < "2020-05-01" AND local_date > "2020-03-31") )
  GROUP BY
    s2_id,
    s2_token,
    local_month ) AS t0 ) UNPIVOT ((visits,
    log_visits) FOR visit_score_step IN ((visits_opening,
      log_visits_opening) AS 'opening',
    (visits_original,
      log_visits_original) AS 'original'));