DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");


MERGE INTO `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}` AS target
USING (
  WITH postgres_batch AS (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-batch-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
  )
  , postgres_rt as (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['accessible-dataset'] }}.{{ params['postgres-rt-dataset'] }}-{{ params['sgplacedailyvisitsraw-table'] }}`
  )
  , updated_visits AS (
    SELECT *
    FROM `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-visits-estimation-table'] }}`
  )
  , daily_visits_rt as (  -- explode the visits
    SELECT
      DATE_ADD(local_date, INTERVAL row_number DAY) as local_date
      , CAST(visits as FLOAT64) visits_rt
      , fk_sgplaces
    FROM (
      SELECT
        local_date
        , fk_sgplaces
        , JSON_EXTRACT_ARRAY(visits) AS visit_array -- Get an array from the JSON string
      FROM postgres_rt
    )
    CROSS JOIN 
      UNNEST(visit_array) AS visits -- Convert array elements to row
      WITH OFFSET AS row_number -- Get the position in the array as another column
    ORDER BY
      local_date
      , fk_sgplaces
      , row_number
  )
  , daily_visits_updated_rt as (
    SELECT
      fk_sgplaces
      , local_date
      , IFNULL(updated_visits.visits, daily_visits_rt.visits_rt) as visits
    FROM daily_visits_rt
    LEFT JOIN updated_visits
      USING (fk_sgplaces, local_date)
  )
  , daily_visits_array AS (
    SELECT
      fk_sgplaces
      , local_date
      , TO_JSON_STRING(ARRAY_AGG(IFNULL(visits, 0) ORDER BY local_date_sort)) AS visits
    FROM (
      SELECT
        fk_sgplaces
        , DATE_TRUNC(local_date, MONTH) AS local_date
        , local_date AS local_date_sort
        , visits
      FROM
        daily_visits_updated_rt
    )
    GROUP BY
      fk_sgplaces
      , local_date
  )
  , test_1 AS (  -- all dates which shouldn't be changed, are not changed
    SELECT SUM(wrong_match) AS wrong_matches
    FROM (
      SELECT
        IF(postgres_batch.visits =  daily_visits_array.visits, 0 , 1) AS wrong_match
        , local_date
      FROM postgres_batch
      INNER JOIN daily_visits_array
      USING (local_date, fk_sgplaces)
    )
    WHERE local_date < DATE_SUB(date_to_update, INTERVAL 365 day)
  )
  SELECT * EXCEPT(
    count_old
    , count_new
    , wrong_matches
    , count_old_day_to_update
    , count_new_day_to_update
  )
  FROM daily_visits_array
  CROSS JOIN (
    SELECT COUNT(*) AS count_new
    FROM daily_visits_array
  )
  CROSS JOIN (
    SELECT COUNT(*) AS count_old
    FROM postgres_batch
  )
  CROSS JOIN test_1
  CROSS JOIN (
    SELECT COUNT(*) AS count_old_day_to_update
    FROM daily_visits_rt
    WHERE local_date = date_to_update
  )
  CROSS JOIN (
    SELECT COUNT(*) AS count_new_day_to_update
    FROM daily_visits_updated_rt
    WHERE local_date = date_to_update
  )
  WHERE IF(
    (ABS(count_old - count_new) = 0) AND (wrong_matches = 0) AND (ABS(count_old_day_to_update - count_new_day_to_update) = 0)
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

