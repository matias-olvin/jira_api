DECLARE date_to_update DATE;
SET date_to_update = DATE("{{ ti.xcom_pull(task_ids='local-date') }}");

BEGIN CREATE TEMP TABLE poi_gt_scaled_table AS (
  WITH poi_gt_scaled_table AS (
    SELECT 
      local_date
      , visits AS poi_gt
      , visits / NULLIF(AVG(visits) OVER (PARTITION BY fk_sgplaces), 0)  AS poi_gt_scaled
      , AVG(visits) OVER (PARTITION BY fk_sgplaces) poi_mean_gt
      , fk_sgplaces
    FROM (
      SELECT
        fk_sgplaces
        , local_date
        , SUM(visits) AS visits
      FROM `{{ var.value.sns_project }}.{{ params['sns-raw-dataset'] }}.{{ params['sns-raw-traffic-formatted-table'] }}`
      GROUP BY 
        fk_sgplaces
        , local_date
    )
  )
  , list_poi_good_latency AS (
    SELECT DISTINCT fk_sgplaces
    FROM poi_gt_scaled_table
    WHERE local_date = date_to_update
  )

    SELECT 
      *
    FROM poi_gt_scaled_table
    INNER JOIN `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['list-sns-pois-table'] }}`
      USING (fk_sgplaces)
    INNER JOIN list_poi_good_latency
      USING (fk_sgplaces)
    INNER JOIN `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['class-pois-sns-business-table'] }}`
      USING (fk_sgplaces)
    WHERE tier <> 3  -- tier 3 didnt seem to work really well

); END;


BEGIN CREATE TEMP TABLE group_gt_scaled_table AS (
  SELECT
    local_date
    , visits / NULLIF(AVG(visits) OVER (PARTITION BY tier_id), 0) AS group_gt_scaled
    , AVG(visits) OVER (PARTITION BY tier_id) group_mean_gt
    , tier_id
    , nb_pois
  FROM
  ( 
    SELECT
      tier_id
      , AVG(nullif(poi_gt_scaled,0)) AS visits
      , local_date
      , count(DISTINCT fk_sgplaces) AS nb_pois
    FROM poi_gt_scaled_table
    GROUP BY 
      tier_id
      , local_date
  )
); END;





BEGIN CREATE TEMP TABLE filters_and_logging AS (
  SELECT
    MAX(nb_pois) max_nb_pois
    , MIN(nb_pois) min_nb_pois
    , COUNT(IF(group_gt_scaled IS NULL, 1, NULL)) AS local_dates_with_null
    , tier_id 
  FROM group_gt_scaled_table
  -- data from 2018
  where local_date >= '2021-01-01'
  GROUP BY tier_id
); END;

-- DELETE BEFORE INSERTING
DELETE FROM `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-groups-gt-scaled-table'] }}`
WHERE
  group_id = "business"
  AND stage = "{{ params['stage'] }}"
  AND local_date = date_to_update
  AND run_date = DATE("{{ ds }}")
;
INSERT INTO `{{ var.value.sns_project }}.{{ params['accessible-by-olvin-dataset'] }}.{{ params['visits-estimation-metrics-dataset'] }}-{{ params['logging-rt-groups-gt-scaled-table'] }}`
SELECT
  *
  , "business" AS group_id
  , "{{ params['stage'] }}" AS stage
  , date_to_update AS local_date
  , DATE("{{ ds }}") AS run_date
FROM filters_and_logging;


CREATE OR REPLACE TABLE `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-gt-scaled-groups-business-table'] }}`
AS (
  SELECT *
  FROM group_gt_scaled_table
  INNER JOIN (
    SELECT tier_id
    FROM filters_and_logging  
    WHERE min_nb_pois > 17
  ) USING  (tier_id)
);



CREATE OR REPLACE TABLE
  `{{ var.value.sns_project }}.{{ params['visits-estimation-supervised-dataset'] }}_{{ params['stage'] }}.{{ params['rt-groups-business-class-1-table'] }}`
  -- `sns-vendor-olvin-poc.visits_estimation_ground_truth_supervised_staging.rt_group_business_class_1_pois`

AS (
  SELECT distinct tier_id, fk_sgplaces
  FROM poi_gt_scaled_table
  where tier = 1

);