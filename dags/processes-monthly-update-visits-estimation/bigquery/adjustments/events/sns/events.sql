create or replace table
  `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['factor_tier_table'] }}`
as
WITH

visits_on_date AS (
  SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
  FROM (
    SELECT fk_sgplaces, tier_id, local_date, identifier, SUM(visits) AS visits
    FROM 
      `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}`
    INNER JOIN 
      `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}`
      USING(fk_sgplaces, local_date)
    GROUP BY fk_sgplaces, tier_id, local_date, identifier
  )
  WHERE visits > 0
  GROUP BY tier_id, identifier, local_date
),

reference_visits AS (
  SELECT tier_id, identifier, local_date, APPROX_QUANTILES(med_visits, 100)[OFFSET(50)] AS ref_visits
  FROM (
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM 
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week4back =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
    UNION ALL
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM 
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week3back =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
    UNION ALL
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM 
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week2back =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
    UNION ALL
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM 
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week1back =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
    UNION ALL
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week1 =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
    UNION ALL
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM 
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week2 =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
    UNION ALL
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM 
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week3 =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
    UNION ALL
      SELECT tier_id, identifier, local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] AS med_visits
      FROM (
        SELECT tier_id, identifier, a.local_date, SUM(visits) AS visits
        FROM 
          `{{ var.value.sns_project }}.{{ var.json.visits_estimation_datasets.visits_estimation_adjustments_events_dataset }}.{{ params['tier_dates_table'] }}` a
        INNER JOIN 
          `{{ var.value.sns_project }}.{{ params['sns_raw_dataset'] }}.{{ params['raw_traffic_formatted_table'] }}` b
          ON a.fk_sgplaces=b.fk_sgplaces
          AND a.ref_dates.week4 =b.local_date
        GROUP BY a.fk_sgplaces, tier_id, a.local_date, identifier
      )
      WHERE visits > 0
      GROUP BY tier_id, identifier, local_date
  )
  GROUP BY tier_id, identifier, local_date
)


    SELECT tier_id, identifier, local_date, 
            CASE WHEN med_visits = 0 THEN 0
                ELSE CASE WHEN ref_visits = 0 THEN 1
                          ELSE med_visits / ref_visits 
                          END 
                END AS factor
    FROM visits_on_date
    INNER JOIN reference_visits
      USING(tier_id, identifier, local_date)
;
