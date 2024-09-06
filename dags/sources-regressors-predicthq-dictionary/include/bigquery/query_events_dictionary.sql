CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_regressors_dataset'] }}.{{ params['predicthq_events_dictionary_table']}}` AS
WITH
  s2_token_table AS (
    SELECT
      pid AS fk_sgplaces,
      TO_HEX(
        CAST(
          ( --Final result in hexadecimal
            SELECT
              STRING_AGG(
                CAST(
                  S2_CELLIDFROMPOINT(long_lat_point, {{ s2_token_size }}) >> bit & 0x1 AS STRING
                ),
                ''
                ORDER BY
                  bit DESC
              )
            FROM
              UNNEST (GENERATE_ARRAY(0, 63)) AS bit
          ) AS BYTES FORMAT "BASE2"
        )
      ) AS s2_token
    FROM
      `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  )
SELECT
  fk_sgplaces,
  CONCAT('s2_token_', s2_token, '_phq_attendance_sports') AS identifier_phq_attendance_sports,
  CONCAT('s2_token_', s2_token, '_avg_local_rank_sports') AS identifier_avg_local_rank_sports,
  CONCAT('s2_token_', s2_token, '_number_of_events_sports') AS identifier_number_of_events_sports,
  CONCAT('s2_token_', s2_token, '_phq_attendance_concerts') AS identifier_phq_attendance_concerts,
  CONCAT('s2_token_', s2_token, '_avg_local_rank_concerts') AS identifier_avg_local_rank_concerts,
  CONCAT(
    's2_token_',
    s2_token,
    '_number_of_events_concerts'
  ) AS identifier_number_of_events_concerts,
  CONCAT(
    's2_token_',
    s2_token,
    '_phq_attendance_festivals'
  ) AS identifier_phq_attendance_festivals,
  CONCAT(
    's2_token_',
    s2_token,
    '_avg_local_rank_festivals'
  ) AS identifier_avg_local_rank_festivals,
  CONCAT(
    's2_token_',
    s2_token,
    '_number_of_events_festivals'
  ) AS identifier_number_of_events_festivals,
  CONCAT(
    's2_token_',
    s2_token,
    '_phq_attendance_performing_arts'
  ) AS identifier_phq_attendance_performing_arts,
  CONCAT(
    's2_token_',
    s2_token,
    '_avg_local_rank_performing_arts'
  ) AS identifier_avg_local_rank_performing_arts,
  CONCAT(
    's2_token_',
    s2_token,
    '_number_of_events_performing_arts'
  ) AS identifier_number_of_events_performing_arts,
  CONCAT(
    's2_token_',
    s2_token,
    '_phq_attendance_conferences'
  ) AS identifier_phq_attendance_conferences,
  CONCAT(
    's2_token_',
    s2_token,
    '_avg_local_rank_conferences'
  ) AS identifier_avg_local_rank_conferences,
  CONCAT(
    's2_token_',
    s2_token,
    '_number_of_events_conferences'
  ) AS identifier_number_of_events_conferences,
  CONCAT(
    's2_token_',
    s2_token,
    '_phq_attendance_community'
  ) AS identifier_phq_attendance_community,
  CONCAT(
    's2_token_',
    s2_token,
    '_avg_local_rank_community'
  ) AS identifier_avg_local_rank_community,
  CONCAT(
    's2_token_',
    s2_token,
    '_number_of_events_community'
  ) AS identifier_number_of_events_community,
  CONCAT('s2_token_', s2_token, '_phq_attendance_expos') AS identifier_phq_attendance_expos,
  CONCAT('s2_token_', s2_token, '_avg_local_rank_expos') AS identifier_avg_local_rank_expos,
  CONCAT('s2_token_', s2_token, '_number_of_events_expos') AS identifier_number_of_events_expos,
FROM
  s2_token_table;