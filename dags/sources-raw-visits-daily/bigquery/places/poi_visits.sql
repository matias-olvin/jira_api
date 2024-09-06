WITH
  places_and_footprints AS (
  SELECT
    places.pid AS fk_sgplaces,
    places.fk_sgbrands,
    places.naics_code,
    places.simplified_wkt_10_buffer,
    wa.week_array,
    places.enclosed,
    places.iso_country_code AS country,
    places.parent_bool,
    places.child_bool
  FROM
    -- `storage-prod-olvin-com.sg_places.20211101` places
        `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{params['places_data_table']}}` places
  LEFT JOIN
     `{{ params['project'] }}.{{ params['places_data_dataset'] }}.{{ params['places_week_array_data_table'] }}` wa
    -- `storage-prod-olvin-com.sg_places.all_week_array-20211101_2` wa
  ON
    places.pid = wa.pid
    WHERE ((places.opening_date <= "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}" ) OR (places.opening_date is null) )
    AND
    ((places.closing_date > "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}" ) OR (places.closing_date is null))
    ),
  clusters_pre AS (
  SELECT
    device_id,
    device_os,
    duration,
    lat_long_visit_point,
    local_date,
    local_hour,
    UNIX_SECONDS(TIMESTAMP(visit_ts)) AS hour_ts,
    TIMESTAMP(visit_ts) AS visit_ts
  FROM
    -- `storage-prod-olvin-com.device_clusters.*`
        `{{ params['project'] }}.{{ params['device_clusters_dataset'] }}.*`
  WHERE
    -- local_date = '2021-01-01' --and ST_DWITHIN( lat_long_visit_point, ST_GEOGPOINT(-82.454241, 27.29955), 200000)
          local_date = "{{ execution_date.subtract(days=(var.value.latency_days_visits|int+1)).strftime('%Y-%m-%d') }}"-- Before midnight
    ),
  clusters_post AS (
  SELECT
    device_id,
    device_os,
    duration,
    lat_long_visit_point,
    local_date,
    local_hour,
    UNIX_SECONDS(TIMESTAMP(visit_ts)) AS hour_ts,
    TIMESTAMP(visit_ts) AS visit_ts,
    COUNT(*) OVER (PARTITION BY local_date, device_id, s2_token) AS n_clusters,
    accuracy,
    confidence,
    n_hits,
    publisher_id,
  FROM (
    SELECT
      *,
      `{{ params['project'] }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 8) AS s2_token
      -- TO_HEX(CAST(( -- We want the final result in hexadecimal
      --     SELECT
      --       STRING_AGG( CAST(S2_CELLIDFROMPOINT(lat_long_visit_point,
      --             8) >> bit & 0x1 AS STRING), ''
      --       ORDER BY
      --         bit DESC) -- S2_CELLIDFROMPOINT returns an integer, convert to binary. 8 is the level of the cell
      --     FROM
      --       UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
      --       ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string, BYTES format is required to use TO_HEX
      --     ) ) AS s2_token
    FROM
            `{{ params['project'] }}.{{ params['device_clusters_dataset'] }}.*`
      -- `storage-prod-olvin-com.device_clusters.*`
    WHERE
      -- local_date = '2021-01-02' -- and ST_DWITHIN( lat_long_visit_point, ST_GEOGPOINT(-82.454241, 27.29955), 200000)
            local_date = "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}"
      ) ),
  clusters_joined AS (
  SELECT
    clusters_post.device_id,
    clusters_post.device_os,
    clusters_post.lat_long_visit_point,
    clusters_post.visit_ts,
    clusters_post.local_date
  FROM
    clusters_pre
  JOIN
    clusters_post
  ON
    clusters_pre.device_id = clusters_post.device_id
    AND clusters_pre.device_os = clusters_post.device_os
    AND ST_DWITHIN(clusters_pre.lat_long_visit_point,
      clusters_post.lat_long_visit_point,
      40)
    AND (clusters_pre.hour_ts + clusters_pre.duration + 7200) <= clusters_post.hour_ts
    AND (clusters_pre.duration > 0
      OR clusters_post.duration > 0)
    AND clusters_post.local_hour < 2 ),
  clean_vll AS (
  SELECT
    * EXCEPT(geohash_7,
      geohash_8,
      geohash_9),
    COUNT(*) OVER (PARTITION BY geohash_7, local_hour) AS n_cluster_geohash_7,
    COUNT(*) OVER (PARTITION BY geohash_8, local_hour) AS n_cluster_geohash_8,
    COUNT(*) OVER (PARTITION BY geohash_9, local_hour) AS n_cluster_geohash_9,
  FROM (
    SELECT
      *,
      ST_GEOHASH(lat_long_visit_point,
        7) AS geohash_7,
      ST_GEOHASH(lat_long_visit_point,
        8) AS geohash_8,
      ST_GEOHASH(lat_long_visit_point,
        9) AS geohash_9,
    FROM (
      SELECT
        clusters_post.* EXCEPT(hour_ts)
      FROM
        clusters_post AS clusters_post
      LEFT JOIN
        clusters_joined
      ON
        clusters_post.device_id = clusters_joined.device_id
        AND clusters_post.device_os = clusters_joined.device_os
        AND clusters_post.visit_ts = clusters_joined.visit_ts
        AND ST_EQUALS(clusters_post.lat_long_visit_point,
          clusters_joined.lat_long_visit_point)
        AND clusters_post.local_date = clusters_joined.local_date
      WHERE
        clusters_joined.device_id IS NULL) -- Equivalent to anti join
      ) ),
  join_places_to_visits_polygons AS (
  SELECT
    clean_vll.*,
    EXTRACT(DAYOFWEEK
    FROM
      local_date) AS day_of_week,
    week_array,
    fk_sgbrands,
    naics_code,
    simplified_wkt_10_buffer,
    fk_sgplaces,
    enclosed,
    country,
    parent_bool,
    child_bool
  FROM
    clean_vll
  JOIN
    places_and_footprints
  ON
    ST_WITHIN(lat_long_visit_point,
      simplified_wkt_10_buffer) ),
  add_opening_hours AS (
  SELECT
    * EXCEPT(week_array),
    week_array[
  OFFSET
    (hour_week)] AS poi_opened
  FROM (
    SELECT
      *,
      (day_of_week -1 ) * 24 + local_hour AS hour_week
    FROM
      join_places_to_visits_polygons
      ) ),
  visits_score AS (
  SELECT
    *,
    1/COUNT(*) OVER (PARTITION BY visit_ts, device_id, parent_bool ) AS visit_score
  FROM
    add_opening_hours ),
  visits_score_weighted_by_opening_times AS (
  SELECT
    *,
    1/COUNT(*) OVER (PARTITION BY visit_ts, device_id, poi_opened, parent_bool ) AS visit_score_weighted
  FROM
    visits_score ),
  visits_score_clean AS (
  SELECT
    *,
    UNIX_SECONDS(TIMESTAMP_TRUNC(TIMESTAMP(visit_ts), HOUR)) AS hour_ts,
    CASE poi_opened
      WHEN 0 THEN 0
    ELSE
    visit_score_weighted
  END
    AS visit_score_opening
  FROM
    visits_score_weighted_by_opening_times ),
  clean_output AS (
  SELECT
    device_id,
    duration,
    lat_long_visit_point,
    local_date,
    local_hour,
    visit_ts,
    publisher_id,
    country,
    device_os,
    STRUCT(accuracy,
      confidence,
      n_clusters,
      n_hits,
      n_cluster_geohash_7,
      n_cluster_geohash_8,
      n_cluster_geohash_9) AS quality_stats,
    day_of_week,
    fk_sgbrands,
    naics_code,
    fk_sgplaces,
    enclosed,
    hour_week,
    STRUCT(visit_score AS original,
      visit_score_weighted AS weighted,
      visit_score_opening AS opening) AS visit_score,
    hour_ts,
    visits_score_clean.parent_bool,
    visits_score_clean.child_bool
  FROM
    visits_score_clean ),
  parent_analysis AS (
  SELECT
    device_id,
    TIMESTAMP_DIFF(TIMESTAMP_ADD(max_visit_ts, INTERVAL max_visit_ts_duration SECOND), min_visit_ts, SECOND) AS duration,
    ANY_VALUE(lat_long_visit_point)lat_long_visit_point,
    local_date,
    MIN(local_hour) local_hour,
    MIN(visit_ts) visit_ts,
    publisher_id,
    country,
    device_os,
    STRUCT(AVG(quality_stats.accuracy) AS accuracy,
      AVG(quality_stats.confidence) AS confidence,
      CAST(AVG(quality_stats.n_clusters) as INT64) AS clusters,
      CAST(AVG(quality_stats.n_hits)as INT64)  AS n_hits,
      CAST(AVG(quality_stats.n_cluster_geohash_7)as INT64)  AS n_cluster_geohash_7,
      CAST(AVG(quality_stats.n_cluster_geohash_8)as INT64)  AS n_cluster_geohash_8,
      CAST(AVG(quality_stats.n_cluster_geohash_9)as INT64)  AS n_cluster_geohash_9) AS quality_stats,
    day_of_week,
    fk_sgbrands,
    naics_code,
    fk_sgplaces,
    enclosed,
    MIN(hour_week) hour_week,
    STRUCT(MAX(visit_score.weighted) AS weighted,
      MAX(visit_score.original) AS original,
      MAX(visit_score.opening) AS opening) AS visit_score,
    MIN(hour_ts) AS hour_ts,
    parent_bool,
    child_bool
  FROM (
    SELECT
      *,
      FIRST_VALUE(duration) OVER (PARTITION BY fk_sgplaces, parent_bool, local_date, device_id ORDER BY visit_ts DESC) max_visit_ts_duration,
      MIN(visit_ts) OVER (PARTITION BY fk_sgplaces, parent_bool, local_date, device_id) min_visit_ts,
      MAX(visit_ts) OVER (PARTITION BY fk_sgplaces, parent_bool, local_date, device_id) max_visit_ts
    FROM
      clean_output
    WHERE
      clean_output.parent_bool = TRUE)
  GROUP BY
    fk_sgplaces,
    local_date,
    device_id,
    parent_bool,
    max_visit_ts,
    max_visit_ts_duration,
    min_visit_ts,
    publisher_id,
    country,
    device_os,
    day_of_week,
    fk_sgbrands,
    naics_code,
    enclosed,
    child_bool ),
  join_parents_back_to_children AS (
  SELECT
    *
  FROM
    clean_output
  WHERE
    parent_bool IS NOT TRUE
  UNION ALL
  SELECT
    *
  FROM
    parent_analysis )
SELECT
  *
FROM
  join_parents_back_to_children  