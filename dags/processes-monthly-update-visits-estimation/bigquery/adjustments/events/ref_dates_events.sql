CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['tier_events_table'] }}`
AS

SELECT  tier_id, fk_sgplaces, identifier, local_date,
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
INNER JOIN
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['class_pois_sns_table'] }}`
USING(fk_sgplaces)
INNER JOIN
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['list_sns_pois_table'] }}`
USING(fk_sgplaces)