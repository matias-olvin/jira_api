CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_ref_visits_places_table'] }}`
AS

SELECT ref_visits.*
FROM(
    SELECT a.fk_sgplaces, identifier, a.local_date, APPROX_QUANTILES(visits, 100)[OFFSET(50)] as ref_visits
    FROM(
      SELECT fk_sgplaces, identifier, local_date,
            STRUCT(
              DATE_SUB(local_date, INTERVAL 4 WEEK) AS week4back,
              DATE_SUB(local_date, INTERVAL 3 WEEK) AS week3back,
              DATE_SUB(local_date, INTERVAL 2 WEEK) AS week2back,
              DATE_SUB(local_date, INTERVAL 1 WEEK) AS week1back,
              DATE_ADD(local_date, INTERVAL 1 WEEK) AS week1,
              DATE_ADD(local_date, INTERVAL 2 WEEK) AS week2,
              DATE_ADD(local_date, INTERVAL 3 WEEK) AS week3,
              DATE_ADD(local_date, INTERVAL 4 WEEK) AS week4
            ) AS ref_dates
      FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_poi_table'] }}`
    --    `storage-prod-olvin-com.visits_estimation.events_holidays_poi`
    ) a
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}` b
    --    `storage-prod-olvin-com.visits_estimation.quality_output` b
      ON a.fk_sgplaces = b.fk_sgplaces
      AND (a.ref_dates.week4back = b.local_date
        OR a.ref_dates.week3back = b.local_date
        OR a.ref_dates.week2back = b.local_date
        OR a.ref_dates.week1back = b.local_date
        OR a.ref_dates.week4 = b.local_date
        OR a.ref_dates.week3 = b.local_date
        OR a.ref_dates.week2 = b.local_date
        OR a.ref_dates.week1 = b.local_date
      )
    GROUP BY fk_sgplaces, identifier, local_date
) ref_visits
INNER JOIN
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['quality_output_table'] }}`
--    `storage-prod-olvin-com.visits_estimation.quality_output`
  USING(fk_sgplaces, local_date)