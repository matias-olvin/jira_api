DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
WHERE run_date = '{{ ds }}'
AND block='1';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
WITH
  places_visits_table AS (
  SELECT
    *
  FROM (
    SELECT
      -- CAST(NULL AS FLOAT64) AS historical_visits_geoscaling_probability,
      -- CAST(NULL AS FLOAT64) AS historical_visits_geoscaling_global,
      SUM(visit_score_steps.visit_share) AS historical_visits_visit_share,
      fk_sgplaces,
      fk_sgbrands
    FROM
    --   `storage-prod-olvin-com.smc_poi_initial_scaling.2018`
     `{{ var.value.env_project }}.{{ params['poi_visits_block_1_dataset'] }}.2018`
    GROUP BY
      fk_sgplaces,
      fk_sgbrands )
  INNER JOIN (
    SELECT
      pid AS fk_sgplaces,
      standalone_bool
    FROM
    --   `storage-prod-olvin-com.sg_places.20211101`
     `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`)
  USING
    (fk_sgplaces) ),
  median_visits_per_standalone_child AS (
  SELECT
    DISTINCT fk_sgbrands,
    standalone_bool,
    -- CAST(NULL AS FLOAT64) AS median_historical_visits_geoscaling_probability,
    -- CAST(NULL AS FLOAT64) AS median_historical_visits_geoscaling_global,
    PERCENTILE_CONT(historical_visits_visit_share,
      0.5) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS median_historical_visits_visit_share,
    -- CAST(NULL AS FLOAT64) AS stddev_historical_visits_geoscaling_probability,
    -- CAST(NULL AS FLOAT64) AS stddev_historical_visits_geoscaling_global,
    STDDEV(historical_visits_visit_share) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS stddev_historical_visits_visit_share,
    COUNT(*) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS count_elements
  FROM
    places_visits_table )
SELECT
  *, DATE('{{ ds }}') as run_date
FROM (
  SELECT
    *,
    "1" AS block
  FROM
    median_visits_per_standalone_child
  LEFT JOIN (
    SELECT
      pid AS fk_sgbrands,
      name
    FROM
    --   `storage-prod-olvin-com.sg_places.brands` 
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}`)
  USING
    (fk_sgbrands)) UNPIVOT ((median_historical_visits,
      stddev_historical_visits) FOR visit_score_step IN (
        -- (median_historical_visits_geoscaling_probability,
        -- stddev_historical_visits_geoscaling_probability) AS 'geoscaling_probability',
        -- (median_historical_visits_geoscaling_global,
        -- stddev_historical_visits_geoscaling_global) AS 'geoscaling_global',
        (median_historical_visits_visit_share,
        stddev_historical_visits_visit_share) AS 'visit_share'));