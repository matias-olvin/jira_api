DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
WHERE run_date = '{{ ds }}' 
AND block='1';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['grid_volume_vis_table'] }}`
SELECT
  *, DATE('{{ ds }}') as run_date
FROM (
  SELECT
    bqcartoeu.s2.ST_BOUNDARY(s2_id) AS polygon,
    t0.s2_id,
    t0.s2_token,
    -- t0.visits_geoscaling_probability,
    -- t0.log_visits_geoscaling_probability,
    -- t0.visits_geoscaling_global,
    -- t0.log_visits_geoscaling_global,
    -- NULL AS visits_geoscaling_probability,
    -- NULL AS log_visits_geoscaling_probability,
    -- NULL AS visits_geoscaling_global,
    -- NULL AS log_visits_geoscaling_global,
    t0.visits_visit_share,
    t0.log_visits_visit_share,
    t0.local_month,
    "1" AS block
  FROM (
    SELECT
      s2_id,
      s2_token,
      local_month,
      SUM(visits_visit_share) AS visits_visit_share,
      LOG(nullif(SUM(visits_visit_share),
          0)) AS log_visits_visit_share, 
    FROM (
      SELECT
        -- visit_score_steps.geoscaling_probability AS visits_geoscaling_probability,
        -- visit_score_steps.geoscaling_global AS visits_geoscaling_global,
        visit_score_steps.visit_share AS visits_visit_share,
        DATE_TRUNC(local_date, MONTH) AS local_month,
        `{{ var.value.env_project }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 9) AS s2_token,
        S2_CELLIDFROMPOINT(lat_long_visit_point,
          level => 9) AS s2_id,
        -- TO_HEX(CAST((
        --     SELECT
        --       STRING_AGG( CAST(S2_CELLIDFROMPOINT(lat_long_visit_point,
        --             level => 9) >> bit & 0x1 AS STRING), ''
        --       ORDER BY
        --         bit DESC)
        --     FROM
        --       UNNEST(GENERATE_ARRAY(0, 63)) AS bit ) AS BYTES FORMAT "BASE2" ) ) AS s2_token,
      FROM
        `{{ var.value.env_project }}.{{ params['poi_visits_block_1_dataset'] }}.*`
      WHERE
        (local_date < "2021-02-01" AND local_date > "2020-12-31") OR (local_date < "2020-05-01" AND local_date > "2020-03-31") )
  GROUP BY
    s2_id,
    s2_token,
    local_month ) AS t0 ) UNPIVOT ((visits,
    log_visits) FOR visit_score_step IN (
      -- (visits_geoscaling_probability,
      -- log_visits_geoscaling_probability) AS 'geoscaling_probability',
      -- (visits_geoscaling_global,
      -- log_visits_geoscaling_global) AS 'geoscaling_global',
      (visits_visit_share,
      log_visits_visit_share) AS 'visit_share'));