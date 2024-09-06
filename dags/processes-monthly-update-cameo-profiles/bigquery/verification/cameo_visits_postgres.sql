CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_visits_table'] }}_check_postgres` AS
WITH org_table_all AS (
  SELECT * FROM  `{{ var.value.env_project }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_visits_table'] }}`
  INNER JOIN (SELECT pid FROM `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['places_table'] }}`)
    ON fk_sgplaces = pid
  WHERE local_date >= "2019-01-01" AND local_date NOT IN ("2020-04-01", "2020-05-01")
  AND local_date < DATE_ADD("{{ ds }}", INTERVAL 1 MONTH)
),
org_table AS (
  SELECT * FROM org_table_all
  WHERE local_date < DATE_TRUNC("{{ ds }}", MONTH)
),
org_table_date AS (
  SELECT * FROM org_table_all
  WHERE local_date = DATE_TRUNC("{{ ds }}", MONTH)
),

total_date_table AS (
  SELECT
  AVG(total_date) AS date_count_anomaly_avg,
  STDDEV(total_date) AS date_count_anomaly_std,
  AVG(total_places) AS date_place_anomaly_avg,
  STDDEV(total_places) AS date_place_anomaly_std,
  AVG(avg_score) AS date_score_anomaly_avg,
  STDDEV(avg_score) AS date_score_anomaly_std,
  FROM
  (
    SELECT local_date, COUNT(*) AS total_date, SUM(visit_score) AS total_score, AVG(visit_score) AS avg_score,
    COUNT(DISTINCT fk_sgplaces) AS total_places
    FROM org_table
    GROUP BY local_date
  )
),
avg_place_table AS (
  SELECT
  AVG(avg_place_score) AS place_score_anomaly_avg,
  STDDEV(avg_place_score) AS place_score_anomaly_std,
  AVG(avg_cameo_place) AS place_cameo_anomaly_avg,
  STDDEV(avg_cameo_place) AS place_cameo_anomaly_std,
  FROM
  (
    SELECT
    local_date,
    AVG(total_score_place) AS avg_place_score,
    AVG(cameo_place) AS avg_cameo_place,

    FROM
    (
      SELECT local_date, SUM(visit_score) AS total_score_place,
      COUNT(DISTINCT CAMEO_USA) AS cameo_place
      FROM org_table
      GROUP BY local_date, fk_sgplaces
    )
    GROUP BY local_date
  )
),

total_date_table_date AS (
  SELECT
  local_date,
  (IFNULL(total_date, 0) - date_count_anomaly_avg)/date_count_anomaly_std AS date_count_anomaly,
  (IFNULL(total_places, 0) - date_place_anomaly_avg)/date_place_anomaly_std AS date_place_anomaly,
  (IFNULL(avg_score, 0) - date_score_anomaly_avg)/date_score_anomaly_std AS date_score_anomaly
  FROM
  (
    SELECT local_date, COUNT(*) AS total_date, SUM(visit_score) AS total_score, AVG(visit_score) AS avg_score,
    COUNT(DISTINCT fk_sgplaces) AS total_places
    FROM org_table_date
    GROUP BY local_date
  )
  FULL OUTER JOIN (
    SELECT DATE_TRUNC("{{ ds }}", MONTH) AS local_date
  )
  USING(local_date)
  CROSS JOIN total_date_table
),
avg_place_table_date AS (
  SELECT
  local_date,
  (IFNULL(avg_place_score, 0) - place_score_anomaly_avg)/place_score_anomaly_std AS place_score_anomaly,
  (IFNULL(avg_cameo_place, 0) - place_cameo_anomaly_avg)/place_cameo_anomaly_std AS place_cameo_anomaly,
  FROM
  (
    SELECT
    local_date,
    AVG(total_score_place) AS avg_place_score,
    AVG(cameo_place) AS avg_cameo_place,

    FROM
    (
      SELECT local_date, SUM(visit_score) AS total_score_place,
      COUNT(DISTINCT CAMEO_USA) AS cameo_place
      FROM org_table_date
      GROUP BY local_date, fk_sgplaces
    )
    GROUP BY local_date
  )
  FULL OUTER JOIN (
    SELECT DATE_TRUNC("{{ ds }}", MONTH) AS local_date
  )
  USING(local_date)
  CROSS JOIN avg_place_table
)
SELECT *
FROM avg_place_table_date
FULL OUTER JOIN total_date_table_date
USING(local_date)
ORDER BY local_date;
SELECT
*
FROM `{{ var.value.env_project }}.{{ params['cameo_staging_dataset'] }}.{{ params['cameo_visits_table'] }}_check_postgres`
 WHERE
 IF(place_score_anomaly BETWEEN -1.5 AND 1.5, TRUE, ERROR("place score anomaly detected")) AND
 IF(place_cameo_anomaly > -1.5, TRUE, ERROR("place cameo anomaly detected")) AND
 IF(date_count_anomaly > -1.5, TRUE, ERROR("date count anomaly detected")) AND
 IF(date_place_anomaly > -1, TRUE, ERROR("date place anomaly detected")) AND
 IF(date_score_anomaly BETWEEN -1.2 AND 2, TRUE, ERROR("date score anomaly detected"));
