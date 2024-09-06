DELETE `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
WHERE
  run_date = '{{ ds }}'
AND
  block='0';

INSERT INTO `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['standalone_vs_child_vis_table'] }}`
WITH
  places_visits_table AS (
  SELECT
    *
  FROM (
    SELECT
      SUM(visit_score.opening) AS historical_visits_opening,
      SUM(visit_score.original) AS historical_visits_original,
      fk_sgplaces,
      fk_sgbrands
    FROM
    --   `storage-prod-olvin-com.poi_visits.2018`
     `{{ var.value.env_project }}.{{ params['smc_poi_visits_dataset'] }}.2018`
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
    PERCENTILE_CONT(historical_visits_opening,
      0.5) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS median_historical_visits_opening,
    PERCENTILE_CONT(historical_visits_original,
      0.5) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS median_historical_visits_original,
    STDDEV(historical_visits_opening) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS stddev_historical_visits_opening,
    STDDEV(historical_visits_original) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS stddev_historical_visits_original,
    COUNT(*) OVER(PARTITION BY fk_sgbrands, standalone_bool) AS count_elements,
  FROM
    places_visits_table )
SELECT
  *, DATE('{{ ds }}') as run_date
FROM (
  SELECT
    *,
    "0" AS block
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
      stddev_historical_visits) FOR visit_score_step IN ((median_historical_visits_opening,
        stddev_historical_visits_opening) AS 'opening',
        (median_historical_visits_original,
        stddev_historical_visits_original) AS 'original'));