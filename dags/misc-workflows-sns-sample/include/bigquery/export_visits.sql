EXPORT DATA OPTIONS(
  uri="gs://{{ params['sns_transfer_bucket'] }}/export/*.csv",
  format='CSV',
  overwrite=true,
  header=true,
  field_delimiter=';') AS
WITH
  get_visits AS(
  SELECT
    places.pid AS fk_sgplaces,
    visits.local_date,
    visits.visits,
    places.name
  FROM
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplacedailyvisitsraw_table'] }}` visits
  INNER JOIN
    `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` places
  ON
    visits.fk_sgplaces=places.pid
    AND (places.name="Boost Mobile"
      OR places.name="Xfinity"
      OR places.name="T-Mobile")),
  daily_visits AS (
  SELECT
    DATE_ADD(local_date, INTERVAL row_number DAY) AS local_date,
    CAST(visits AS FLOAT64) visits,
    fk_sgplaces,
    name,
    row_number,
  FROM (
    SELECT
      local_date,
      fk_sgplaces,
      name,
      JSON_EXTRACT_ARRAY(visits) AS visit_array,
    FROM
      get_visits )
  CROSS JOIN
    UNNEST(visit_array) AS visits -- Convert ARRAY elements TO ROW
  WITH
  OFFSET
    AS row_number -- Get the position IN the ARRAY AS another COLUMN
  ORDER BY
    local_date,
    fk_sgplaces,
    row_number )
SELECT
  * except(row_number)
FROM
  daily_visits
