DROP MATERIALIZED VIEW `{{ var.value.env_project }}.{{ params['smc_places_dataset'] }}.{{ params['footprints_m_view'] }}`;

CREATE MATERIALIZED VIEW 
    `{{ var.value.env_project }}.{{ params['smc_places_dataset'] }}.{{ params['footprints_m_view'] }}`
AS (
  SELECT
      places.pid AS fk_sgplaces,
      places.fk_sgbrands,
      places.naics_code,
      places.simplified_wkt_10_buffer,
      wa.week_array,
      places.enclosed,
      places.iso_country_code AS country,
      places.parent_bool,
      places.child_bool,
      CAST(places.opening_date AS DATE) AS opening_date,
      CAST(places.closing_date AS DATE) AS closing_date
  FROM
      `{{ var.value.env_project }}.{{ params['smc_places_dataset'] }}.{{params['places_table']}}` places
  JOIN
     `{{ var.value.env_project }}.{{ params['smc_places_dataset'] }}.{{ params['week_array_table'] }}` wa
  ON
    places.pid = wa.pid
)