DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");


MERGE INTO `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacemonthlyvisitsraw-table'] }}` AS target 
USING (
  WITH postgres_monthly_batch AS (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacemonthlyvisitsraw-table'] }}`
  )
  , postgres_monthly_rt AS (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacemonthlyvisitsraw-table'] }}`
  )
  , updated_visits AS (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-table'] }}`
  )
  , postgres_daily_rt AS (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
    where local_date = DATE_TRUNC(date_to_update, month)
  )
  , daily_visits_rt AS (  -- explode the visits
    SELECT
      DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date
      , CAST(visits AS FLOAT64) rt_visits
      , fk_sgplaces
    FROM (
      SELECT
        local_date
        , fk_sgplaces
        , JSON_EXTRACT_ARRAY(visits) AS visit_array  -- Get an array FROM the JSON string
      FROM postgres_daily_rt
    )
    CROSS JOIN 
      UNNEST(visit_array) AS visits -- Convert array elements to row
      WITH OFFSET AS row_number -- Get the position in the array AS another column
    ORDER BY
      local_date
      , fk_sgplaces
      , row_number
  )
  , postgres_monthly_rt_new AS (
    SELECT
      DATE_TRUNC(local_date, MONTH) AS local_date
      , fk_sgplaces
      , SUM(visits) AS visits
    FROM (
      SELECT
        fk_sgplaces
        , local_date
        , IFNULL(updated_visits.visits, daily_visits_rt.rt_visits) AS visits
      FROM daily_visits_rt
      LEFT JOIN updated_visits
        USING (fk_sgplaces, local_date)
    )
    GROUP BY 1,2
  )
  , rt_monthly_visits_updated AS (
    SELECT
      local_date
      , fk_sgplaces
      , CAST(IFNULL(postgres_monthly_rt_new.visits, postgres_monthly_rt.visits) AS INT64) AS visits
    FROM postgres_monthly_rt
    LEFT JOIN postgres_monthly_rt_new
      USING (local_date, fk_sgplaces)
  )
  , test_non_updated AS (  -- all dates which shouldn't be changed, are not changed
    SELECT SUM(wrong_match) AS wrong_matches
    FROM (
      SELECT
        IF(postgres_monthly_batch.visits =  rt_monthly_visits_updated.visits, 0 , 1) AS wrong_match
        , local_date
      FROM postgres_monthly_batch
      INNER JOIN rt_monthly_visits_updated
      USING (local_date, fk_sgplaces)
    )
    WHERE local_date < DATE_SUB(DATE_TRUNC(date_to_update, MONTH), INTERVAL 365 DAY)
  )
  SELECT * EXCEPT(
    count_old
    , count_new
    , wrong_matches
    , count_old_day_to_update
    , count_new_day_to_update
  )
  FROM rt_monthly_visits_updated
  CROSS JOIN (
      SELECT COUNT(*) AS count_new
      FROM rt_monthly_visits_updated
  )
  CROSS JOIN (
    SELECT COUNT(*) AS count_old
    FROM postgres_monthly_batch
  )
  CROSS JOIN test_non_updated
  CROSS JOIN (
    SELECT COUNT(*) AS count_old_day_to_update
    FROM postgres_monthly_rt_new
    WHERE local_date = DATE_TRUNC(date_to_update, MONTH)
  )
  CROSS JOIN (
    SELECT COUNT(*) AS count_new_day_to_update
    FROM rt_monthly_visits_updated
    WHERE local_date = DATE_TRUNC(date_to_update, MONTH)
  )
  WHERE IF(
    (ABS(count_old - count_new) = 0) AND  (wrong_matches = 0) AND ((count_old_day_to_update - count_new_day_to_update) < 0)  -- because of the extra ones USING generic shapes
    , TRUE
    , ERROR(FORMAT(
      "Number of rows must be equal in source and destination tables, but source has %d rows and destination has %d rows. Or no same values on un-updated %d rows. Number of rows must be equal in source and destination tables on date to update, but source has %d rows and destination has %d rows."
      , count_old
      , count_new
      , wrong_matches
      , count_old_day_to_update
      , count_new_day_to_update
    ))
  )
) AS source
ON
  target.fk_sgplaces = source.fk_sgplaces
  AND target.local_date = source.local_date
WHEN MATCHED THEN
  UPDATE SET target.visits = source.visits