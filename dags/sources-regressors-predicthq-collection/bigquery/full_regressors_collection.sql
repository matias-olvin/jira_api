WITH

s2_token_table AS (
  SELECT
    CAST(`end` AS DATE) AS local_date,
    sum(phq_attendance) as phq_attendance,
    AVG(local_rank) as avg_local_rank,
    count(id) as number_of_events,
    category,
    TO_HEX(
      CAST(( --Final result in hexadecimal
        SELECT
          STRING_AGG(CAST(S2_CELLIDFROMPOINT(ST_GEOGPOINT(lon, lat), {{params['s2_token_size']}}) >> bit & 0x1 AS STRING), '' ORDER BY bit DESC)
        FROM UNNEST(GENERATE_ARRAY(0, 63)) AS bit
      ) AS BYTES FORMAT "BASE2")
    ) AS s2_token
  FROM
  `{{ var.value.env_project }}.{{ params['regressors_dataset'] }}.{{ params['full_data_table']}}`
  group by
    s2_token,
    local_date,
    category
),

sports_events AS (
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_phq_attendance_',category) as identifier,
    phq_attendance as factor
  FROM s2_token_table
  WHERE category ='sports'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_avg_local_rank_',category) as identifier,
    avg_local_rank as factor
  FROM s2_token_table
  WHERE category ='sports'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_number_of_events_',category) as identifier,
    number_of_events as factor
  FROM s2_token_table
  WHERE category ='sports'
),

concerts_events AS (
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_phq_attendance_',category) as identifier,
    phq_attendance as factor
  FROM s2_token_table
  WHERE category ='concerts'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_avg_local_rank_',category) as identifier,
    avg_local_rank as factor
  FROM s2_token_table
  WHERE category ='concerts'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_number_of_events_',category) as identifier,
    number_of_events as factor
  FROM s2_token_table
  WHERE category ='concerts'
),

festivals_events AS (
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_phq_attendance_',category) as identifier,
    phq_attendance as factor
  FROM s2_token_table
  WHERE category ='festivals'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_avg_local_rank_',category) as identifier,
    avg_local_rank as factor
  FROM s2_token_table
  WHERE category ='festivals'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_number_of_events_',category) as identifier,
    number_of_events as factor
  FROM s2_token_table
  WHERE category ='festivals'
),

performing_arts_events AS (
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_phq_attendance_',category) as identifier,
    phq_attendance as factor
  FROM s2_token_table
  WHERE category ='performing-arts'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_avg_local_rank_',category) as identifier,
    avg_local_rank as factor
  FROM s2_token_table
  WHERE category ='performing-arts'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_number_of_events_',category) as identifier,
    number_of_events as factor
  FROM s2_token_table
  WHERE category ='performing-arts'
),

conferences_events AS (
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_phq_attendance_',category) as identifier,
    phq_attendance as factor
  FROM s2_token_table
  WHERE category ='conferences'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_avg_local_rank_',category) as identifier,
    avg_local_rank as factor
  FROM s2_token_table
  WHERE category ='conferences'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_number_of_events_',category) as identifier,
    number_of_events as factor
  FROM s2_token_table
  WHERE category ='conferences'
),

community_events AS (
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_phq_attendance_',category) as identifier,
    phq_attendance as factor
  FROM s2_token_table
  WHERE category ='community'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_avg_local_rank_',category) as identifier,
    avg_local_rank as factor
  FROM s2_token_table
  WHERE category ='community'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_number_of_events_',category) as identifier,
    number_of_events as factor
  FROM s2_token_table
    WHERE category ='community'
),

expos_events AS (
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_phq_attendance_',category) as identifier,
    phq_attendance as factor
  FROM s2_token_table
  WHERE category ='expos'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_avg_local_rank_',category) as identifier,
    avg_local_rank as factor
  FROM s2_token_table
  WHERE category ='expos'

  UNION ALL
  SELECT
    s2_token_table.local_date,
    CONCAT('s2_token_',s2_token,'_number_of_events_',category) as identifier,
    number_of_events as factor
  FROM s2_token_table
  WHERE category ='expos'
)

SELECT *
FROM sports_events

UNION ALL
SELECT *
FROM concerts_events

UNION ALL
SELECT *
FROM festivals_events

UNION ALL
SELECT *
FROM performing_arts_events

UNION ALL
SELECT *
FROM conferences_events

UNION ALL
SELECT *
FROM community_events

UNION ALL
SELECT *
FROM expos_events
