BEGIN
CREATE TEMP TABLE base_events AS (
  SELECT
    fk_sgplaces
    , local_date
    , IFNULL(NULLIF(visits_event, 0), quality_output.visits) AS visits
    , IF(NULLIF(visits_event, 0) IS NULL, FALSE, TRUE) AS modified_in_events
  FROM `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}` quality_output
  LEFT JOIN (
    SELECT
      fk_sgplaces
      , local_date
      , IFNULL(factor, 1) * ref_visits AS visits_event
    FROM `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_ref_visits_places_table'] }}`
    LEFT JOIN `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_places_table'] }}`
      USING (fk_sgplaces, local_date)
  ) USING (fk_sgplaces, local_date)
); END;


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['adjustments_events_output_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
AS (
  SELECT * EXCEPT(modified_in_events)
  FROM base_events
);


CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.{{ params['visits_estimation_ground_truth_supervised_dataset'] }}-{{ params['adjustments_events_usage_table'] }}` AS (
  SELECT * EXCEPT(visits)
  FROM base_events
);
