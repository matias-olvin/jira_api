DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
WHERE
  run_date = '{{ ds }}'
AND
  block='daily_estimation';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
WITH
  places_visits_table AS (
  SELECT
    *
  FROM (
    SELECT
      SUM(visit_score_steps.daily_estimation) AS historical_visits_daily_estimation,
      fk_sgplaces,
      fk_sgbrands
    FROM
    --   `storage-prod-olvin-com.smc_poi_visits_scaled_block_groundtruth.2018`
     `{{ var.value.env_project }}.{{ params['poi_visits_block_daily_estimation_dataset'] }}.2018`
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
    PERCENTILE_CONT(historical_visits_daily_estimation,
      0.5) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS median_historical_visits_daily_estimation,
    STDDEV(historical_visits_daily_estimation) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS stddev_historical_visits_daily_estimation,
    COUNT(*) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS count_elements,
  FROM
    places_visits_table )

SELECT
  *, DATE('{{ ds }}') as run_date
FROM (
  SELECT
    *,
    "daily_estimation" AS block
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
      stddev_historical_visits) FOR visit_score_step IN ((median_historical_visits_daily_estimation,
        stddev_historical_visits_daily_estimation) AS 'daily_estimation'));