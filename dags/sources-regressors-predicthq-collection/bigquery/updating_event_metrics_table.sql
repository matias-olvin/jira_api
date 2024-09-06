WITH

s2_token_table AS (
  SELECT
    CAST(`end` AS DATE) AS local_date,
    sum(phq_attendance) as phq_attendance,
    AVG(local_rank) as avg_local_rank,
    count(id) as number_of_events,category,
    cast(updated as DATE) as updated_date,
    TO_HEX(
      CAST(( --Final result in hexadecimal
        SELECT
        STRING_AGG(
          CAST(S2_CELLIDFROMPOINT(ST_GEOGPOINT(lon, lat), {{params['s2_token_size']}}) >> bit & 0x1 AS STRING), '' ORDER BY bit DESC)
          FROM UNNEST(GENERATE_ARRAY(0, 63)) AS bit
      ) AS BYTES FORMAT "BASE2")
    ) AS s2_token
  FROM
    `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['full_data_table'] }}`
  group by
    s2_token,
    local_date,
    category,
    updated
)

SELECT *
FROM s2_token_table
