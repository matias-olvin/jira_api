CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['hourly_visits_table'] }}`
PARTITION BY
  local_date
CLUSTER BY
  fk_sgplaces AS
WITH
  poi_data AS (
    SELECT
      pid AS fk_sgplaces,
      fk_sgbrands,
      open_hours
    FROM
      `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplaceraw_table'] }}`
  ),
  -- Daily Visits
  daily_visits_table AS (
    SELECT
      fk_sgplaces AS poi_to_add,
      local_date,
      visits
    FROM
      `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['daily_visits_table'] }}`
  ),
  -- Create HOURLY visits using avg brand weekly pattern
  brand_pois AS (
    SELECT
      poi_to_add,
      pid AS fk_sgplaces,
      fk_sgbrands,
      polygon_area_sq_ft AS area
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplaceraw_table'] }}`
      INNER JOIN (
        SELECT
          fk_sgplaces AS poi_to_add,
          fk_sgbrands
        FROM
          poi_data
      ) USING (fk_sgbrands)
  ),
  brand_pois_hourly_visits AS (
    SELECT
      poi_to_add,
      a.fk_sgplaces,
      EXTRACT(
        DAYOFWEEK
        FROM
          local_date
      ) AS day_of_week,
      ARRAY (
        SELECT
          CAST(value AS INT64)
        FROM
          UNNEST (
            SPLIT(REPLACE(REPLACE(visits, '[', ''), ']', ''), ',')
          ) AS value
      ) AS visits_array
    FROM
      `{{ var.value.env_project }}.{{ params['postgres_batch_dataset'] }}.{{ params['sgplace_hourly_all_visits_raw_table'] }}` a
      INNER JOIN brand_pois USING (fk_sgplaces)
      INNER JOIN poi_data ON poi_to_add = poi_data.fk_sgplaces
    WHERE
      local_date >= DATE_SUB(CAST('{{ ds }}' AS DATE), INTERVAL 1 YEAR)
      AND local_date < CAST('{{ ds }}' AS DATE)
  ),
  brand_based_week_array AS (
    SELECT
      poi_to_add,
      (day_of_week -1) * 24 + HOUR AS week_hour,
      SUM(CAST(visits AS INT64)) AS total_visits
    FROM
      brand_pois_hourly_visits,
      UNNEST (visits_array) AS visits
    WITH
    OFFSET
      AS HOUR
    GROUP BY
      poi_to_add,
      day_of_week,
      HOUR
  ),
  -- Include poi week_array before normalizing
  split_open_hours_by_day AS (
    SELECT
      open_hours,
      fk_sgplaces,
      ARRAY_TO_STRING(
        SPLIT(
          REGEXP_REPLACE(
            REGEXP_EXTRACT(open_hours, "{.+Tue"),
            r'\], \[',
            ']"Mon"['
          ),
          "Tue"
        ),
        ""
      ) AS Mon_temp,
      ARRAY_TO_STRING(
        SPLIT(
          REGEXP_REPLACE(
            REGEXP_EXTRACT(open_hours, "Tue.+Wed"),
            r'\], \[',
            ']"Tue"['
          ),
          "Wed"
        ),
        ""
      ) AS Tue_temp,
      ARRAY_TO_STRING(
        SPLIT(
          REGEXP_REPLACE(
            REGEXP_EXTRACT(open_hours, "Wed.+Thu"),
            r'\], \[',
            ']"Wed"['
          ),
          "Thu"
        ),
        ""
      ) AS Wed_temp,
      ARRAY_TO_STRING(
        SPLIT(
          REGEXP_REPLACE(
            REGEXP_EXTRACT(open_hours, "Thu.+Fri"),
            r'\], \[',
            ']"Thu"['
          ),
          "Fri"
        ),
        ""
      ) AS Thu_temp,
      ARRAY_TO_STRING(
        SPLIT(
          REGEXP_REPLACE(
            REGEXP_EXTRACT(open_hours, "Fri.+Sat"),
            r'\], \[',
            ']"Fri"['
          ),
          "Sat"
        ),
        ""
      ) AS Fri_temp,
      ARRAY_TO_STRING(
        SPLIT(
          REGEXP_REPLACE(
            REGEXP_EXTRACT(open_hours, "Sat.+Sun"),
            r'\], \[',
            ']"Sat"['
          ),
          "Sun"
        ),
        ""
      ) AS Sat_temp,
      ARRAY_TO_STRING(
        SPLIT(
          REGEXP_REPLACE(
            REGEXP_EXTRACT(open_hours, "Sun.+"),
            r'\], \[',
            ']"Sun"['
          ),
          "*"
        ),
        ""
      ) AS Sun_temp
    FROM
      (
        SELECT
          fk_sgplaces,
          NULLIF(open_hours, '') AS open_hours
        FROM
          poi_data
      )
    WHERE
      open_hours IS NOT NULL
  ),
  concat_output AS (
    SELECT
      fk_sgplaces,
      open_hours,
      CONCAT(
        Mon_temp,
        Tue_temp,
        Wed_temp,
        Thu_temp,
        Fri_temp,
        Sat_temp,
        Sun_temp
      ) AS transformed_open_hours
    FROM
      split_open_hours_by_day
  ),
  split_opening_hours_dict AS (
    SELECT
      *,
      REGEXP_EXTRACT_ALL(transformed_open_hours, r'"(\w+)"') AS day_of_week,
      (
        SELECT
          ARRAY_AGG(CAST(id AS INT64))
        FROM
          UNNEST (
            REGEXP_EXTRACT_ALL(transformed_open_hours, r'\["(\d+):\d+')
          ) id
      ) AS opening_hour,
      (
        SELECT
          ARRAY_AGG(CAST(id AS INT64))
        FROM
          UNNEST (
            REGEXP_EXTRACT_ALL(transformed_open_hours, r'"(\d+):\d+"\]')
          ) id
      ) AS closing_hour
    FROM
      concat_output
  ),
  unnest_array AS (
    SELECT
      fk_sgplaces,
      opening_hour,
      closing_hour,
      `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['map_week_array_table'] }}` (
        day_of_week,
        [
          ("Sun", 0),
          ("Mon", 1),
          ("Tue", 2),
          ("Wed", 3),
          ("Thu", 4),
          ("Fri", 5),
          ("Sat", 6)
        ],
        0
      ) AS day_of_week_int
    FROM
      (
        SELECT
          fk_sgplaces,
          opening_hour,
          closing_hour,
          day_of_week
        FROM
          split_opening_hours_dict,
          UNNEST (day_of_week) day_of_week
        WITH
        OFFSET
          pos1,
          UNNEST (opening_hour) opening_hour
        WITH
        OFFSET
          pos2,
          UNNEST (closing_hour) closing_hour
        WITH
        OFFSET
          pos3
        WHERE
          pos1 = pos2
          AND pos2 = pos3
      )
  ),
  places_opening_hours_convert_to_week AS (
    SELECT
      fk_sgplaces,
      day_of_week_int * 24 + opening_hour AS opening_week_hour,
      day_of_week_int * 24 + closing_hour AS closing_week_hour,
      closing_hour - opening_hour AS open_length
    FROM
      unnest_array
  ),
  transform_hours AS (
    SELECT
      fk_sgplaces,
      opening_week_hour AS before_opening,
      closing_week_hour-opening_week_hour AS opening,
      168 - closing_week_hour AS after_opening
    FROM
      places_opening_hours_convert_to_week
  ),
  concat_hours AS (
    SELECT
      *,
      CONCAT(
        '[0*',
        CAST(before_opening AS STRING),
        ',1*',
        CAST(opening AS STRING),
        ',0*',
        CAST(after_opening AS STRING),
        ']'
      ) AS concat_opening
    FROM
      transform_hours
  ),
  create_week_array AS (
    SELECT
      * EXCEPT (
        before_opening,
        opening,
        after_opening,
        concat_opening
      ),
      `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['expand_list_week_array_table'] }}` (concat_opening) AS week_array
    FROM
      concat_hours
  ),
  places_opening_hours_week_array AS (
    SELECT
      fk_sgplaces,
      ARRAY_AGG(
        x
        ORDER BY
          day_of_week_int
      ) AS week_array
    FROM
      (
        SELECT
          fk_sgplaces,
          day_of_week_int,
          LEAST(SUM(CAST(x AS INT64)), 1) AS x
        FROM
          create_week_array,
          UNNEST (week_array) AS x
        WITH
        OFFSET
          day_of_week_int
        GROUP BY
          fk_sgplaces,
          day_of_week_int
      )
    GROUP BY
      fk_sgplaces
  ),
  pois_to_add_open_hours AS (
    SELECT
      fk_sgplaces AS poi_to_add,
      week_hour,
      open_poi
    FROM
      places_opening_hours_week_array
      CROSS JOIN UNNEST (week_array) AS open_poi
    WITH
    OFFSET
      AS week_hour
  ),
  -- Combine avg brand week array with poi info
  brand_plus_opening_week_array AS (
    SELECT
      poi_to_add,
      week_hour,
      IFNULL(open_poi, 1) * total_visits AS total_visits
    FROM
      brand_based_week_array
      LEFT JOIN pois_to_add_open_hours USING (poi_to_add, week_hour)
  ),
  brand_normalized_week_array AS (
    SELECT
      poi_to_add,
      CAST(week_hour / 24 + 0.5 AS INT64) AS day_of_week,
      MOD(week_hour, 24) AS HOUR,
      norm_hourly_visits
    FROM
      (
        SELECT
          b.poi_to_add,
          week_hour,
          IFNULL(total_visits / NULLIF(daily_visits, 0), 0) AS norm_hourly_visits
        FROM
          (
            SELECT
              poi_to_add,
              CAST(week_hour / 24 + 0.5 AS INT64) AS day_of_week,
              SUM(total_visits) AS daily_visits
            FROM
              brand_plus_opening_week_array
            GROUP BY
              poi_to_add,
              day_of_week
          ) a
          INNER JOIN brand_plus_opening_week_array b ON a.poi_to_add = b.poi_to_add
          AND a.day_of_week = CAST(b.week_hour / 24 + 0.5 AS INT64)
      )
  ),
  hourly_visits_float AS (
    SELECT
      b.poi_to_add,
      b.local_date,
      HOUR,
      visits * norm_hourly_visits AS hourly_visits
    FROM
      brand_normalized_week_array a
      INNER JOIN (
        SELECT
          *
        FROM
          daily_visits_table
      ) b ON a.poi_to_add = b.poi_to_add
      AND a.day_of_week = EXTRACT(
        DAYOFWEEK
        FROM
          local_date
      )
  ),
  remaining_decimals AS (
    SELECT
      poi_to_add,
      local_date,
      HOUR,
      hourly_visits-CAST (hourly_visits -0.49 AS INT64) AS remaining
    FROM
      hourly_visits_float
  ),
  remaining_visits_to_assign_hourly AS (
    SELECT
      poi_to_add,
      local_date,
      visits - sum_hourly AS visits_to_assign
    FROM
      daily_visits_table
      INNER JOIN (
        SELECT
          poi_to_add,
          local_date,
          SUM(CAST(hourly_visits -0.49 AS INT64)) AS sum_hourly
        FROM
          hourly_visits_float
        GROUP BY
          poi_to_add,
          local_date
      ) USING (poi_to_add, local_date)
  ),
  -- Hourly visits
  hourly_visits_table AS (
    SELECT
      poi_to_add AS fk_sgplaces,
      local_date,
      HOUR AS local_hour,
      CAST(hourly_visits -0.49 AS INT64) + IFNULL(visits_add, 0) AS visits
    FROM
      hourly_visits_float
      LEFT JOIN (
        SELECT
          poi_to_add,
          local_date,
          HOUR,
          1 AS visits_add
        FROM
          remaining_visits_to_assign_hourly
          INNER JOIN (
            SELECT
              poi_to_add,
              local_date,
              HOUR,
              ROW_NUMBER() OVER (
                PARTITION BY
                  poi_to_add,
                  local_date
                ORDER BY
                  remaining DESC
              ) AS ordinal_
            FROM
              remaining_decimals
          ) USING (poi_to_add, local_date)
        WHERE
          ordinal_ <= visits_to_assign
      ) USING (poi_to_add, local_date, HOUR)
    ORDER BY
      local_date,
      HOUR
  )
SELECT
  *
FROM
  hourly_visits_table