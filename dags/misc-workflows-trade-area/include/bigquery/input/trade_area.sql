create or replace table `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['trade_area_input_table'] }}`
partition by run_date
cluster by fk_sgplaces
as
with places_filter as (
SELECT fk_sgplaces, home_point, visit_score, DATE("{{ ds.format('%Y-%m-01') }}") as run_date FROM `{{ var.value.env_project }}.{{ params['trade_area_dataset'] }}.{{ params['raw_homes_table'] }}`
WHERE fk_sgplaces IN (SELECT 
                      pid 
                      FROM `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`)),
HomePointCounts as (SELECT
    fk_sgplaces,
    COUNT(DISTINCT ST_ASTEXT(home_point)) AS num_home_points,
    COUNTIF(visit_score <> 0.0) AS non_zero_visit_count
  FROM
    places_filter
  GROUP BY
    fk_sgplaces)
SELECT
  t.fk_sgplaces,
  ST_ASTEXT(t.home_point) AS home_point,
  t.visit_score,
  t.run_date
FROM
  places_filter t
JOIN
  HomePointCounts hpc
ON
  t.fk_sgplaces = hpc.fk_sgplaces
WHERE
  hpc.num_home_points > 1 AND hpc.non_zero_visit_count > 1;
