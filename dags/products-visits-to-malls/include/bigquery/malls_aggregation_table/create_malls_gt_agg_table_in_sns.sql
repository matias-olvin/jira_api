CREATE OR REPLACE TABLE
  `{{ var.value.sns_project }}.{{ ti.xcom_pull(task_ids='set_xcom_values.get_poi_matching_dataset') }}.{{ params['malls_gt_agg_table'] }}`
AS

WITH

-- POIs inside Malls
pois_inside_malls AS (
  SELECT
    pid, fk_parents
  FROM 
    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params["sgplaceraw_table"]}}`
  WHERE fk_parents IN (
    SELECT
      pid
    FROM 
      `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ dag_run.conf["dataset_postgres_template"] }}-{{ params["sgcenterraw_table"]}}`
  ) 
),


malls_with_most_gt AS (
  SELECT
    fk_parents,
    COUNT(pid) AS pois_with_gt
  FROM
    pois_inside_malls a
  INNER JOIN (
    SELECT fk_sgplaces
    FROM `{{ var.value.sns_project }}.{{ params["poi_matching_dataset"] }}.{{ params["prod_matching_extended_table"] }}`
    WHERE
   ( date_range_hourly IN ("Highest", "High")
        OR
            (
            date_range_hourly_group IN ("Highest", "High") AND date_density_group IN ("Highest", "High")
            )
        )
    and
  hours_per_day_metric = "Highest" and
  accuracy IN ("Highest", "High") and
  dates_density in ("High", "Highest") and
  (consistency_batch_daily_feed = "Highest" OR consistency_batch_daily_feed IS NULL)
  ) b
    ON a.pid=b.fk_sgplaces
  GROUP BY fk_parents
  HAVING pois_with_gt>4 
),


pois_of_interest AS (
  SELECT
    pid, 
    fk_parents as mall
  FROM
    pois_inside_malls a 
  WHERE
    fk_parents IN(
    SELECT
      fk_parents
    FROM
      malls_with_most_gt)
)

SELECT
  mall,
  local_date,
  COUNT(*) AS num_tenants_with_gt,
  SUM(daily_visits) AS daily_visits_agg
FROM(
  SELECT 
    pid,
    mall,
    local_date,
    SUM(visits) as daily_visits
  FROM 
    `{{ var.value.sns_project }}.{{ params[ "sns_raw_dataset"] }}.{{ params["sns_raw_traffic_formatted_table"] }}`
  INNER JOIN 
    pois_of_interest
  ON fk_sgplaces=pid
  GROUP BY pid, mall, local_date
)
GROUP BY mall, local_date
HAVING num_tenants_with_gt > 1

