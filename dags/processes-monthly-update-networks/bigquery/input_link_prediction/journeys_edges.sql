WITH visits_data AS (
    SELECT 
      device_id, 
      fk_met_areas,
      visit_ts,
      duration,
      fk_nodes,
      visit_score_real,
      point
    FROM
      --`storage-dev-olvin-com.sg_networks_staging.full_visits`
      `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['full_visits_table'] }}`
    ),
  -- sum of visitors - here visits should be renamed to visitors
  visits_totals AS (
    SELECT
      fk_met_areas,
      fk_nodes AS fk_places,
      SUM(visit_score_real) AS total_visits
    FROM (
      SELECT
        device_id,
        fk_met_areas,
        fk_nodes,
        MAX(visit_score_real) AS visit_score_real
      FROM
        visits_data
      WHERE
        fk_nodes != 'home'
      GROUP BY
        device_id,
        fk_met_areas,
        fk_nodes
      )
    GROUP BY
      fk_met_areas,
      fk_nodes
  ),
  -- all pois in that overlapped(or not ) visit
  collect_over_point AS (
    SELECT
      device_id,
      fk_met_areas,
      visit_ts,
      ANY_VALUE(duration) AS duration,
      ARRAY_AGG(CONCAT(fk_nodes, ' ', visit_score_real)) AS all_locations
    FROM
      visits_data
    GROUP BY
      device_id,
      fk_met_areas,
      ST_AsText(point),
      visit_ts
  ),
  -- if we have duration of visit we add it as the end of it
  add_end_time AS (
    SELECT
      *,
      CASE
        WHEN duration IS NOT NULL THEN TIMESTAMP_ADD(visit_ts, INTERVAL duration SECOND) 
        ELSE visit_ts
      END AS visit_end
    FROM
      collect_over_point
  ),
  -- Joining if 6 hours visit from visit and same device
  -- we dont use the visit_end time... Do we want to do it?
  create_visit_pairs AS (
    SELECT
      src_table.device_id,
      src_table.fk_met_areas,
      src_table.all_locations AS src_locations,
      dst_table.all_locations AS dst_locations,
      src_table.visit_ts, 
      dst_table.visit_ts,
      TIMESTAMP_DIFF(dst_table.visit_ts, src_table.visit_ts, HOUR)
    FROM 
      collect_over_point as src_table, collect_over_point as dst_table
    WHERE
      src_table.device_id = dst_table.device_id
    AND
      TIMESTAMP_DIFF(dst_table.visit_ts, src_table.visit_ts, HOUR) > 0
    AND
      TIMESTAMP_DIFF(dst_table.visit_ts, src_table.visit_ts, HOUR) <= CAST("6" AS INT64)
  ),
  -- all combinations of journeys
  visits_explode AS (
    SELECT
      device_id,
      fk_met_areas,
      SPLIT(src, ' ')[OFFSET(0)] AS src_node,
      CAST(SPLIT(src, ' ')[OFFSET(1)] AS FLOAT64) AS src_score,
      SPLIT(dst, ' ')[OFFSET(0)] AS dst_node,
      CAST(SPLIT(dst, ' ')[OFFSET(1)] AS FLOAT64) AS dst_score,
    FROM
      create_visit_pairs,
      UNNEST(src_locations) AS src,
      UNNEST(dst_locations) AS dst
  ),
  -- just one visit per device 
  visits_once_per_device AS (
    SELECT
      fk_met_areas,
      device_id,
      MAX(src_score*dst_score) AS weight,
      src_node,
      dst_node,
    FROM
      visits_explode
    WHERE
      src_node != dst_node
    GROUP BY
      fk_met_areas,
      device_id,
      src_node,
      dst_node
  ),
  -- adding edges where more or equal than threshold devices
  visits_collect_all AS (
    SELECT
      fk_met_areas,
      src_node,
      dst_node,
      weight,
      IF ("{{params['mode']}}" = "historical",
          PARSE_DATE('%F', "{{ execution_date.add(months=1).format('%Y-%m-01') }}"),
          DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}")
          ) AS local_date
    FROM (
      SELECT
        fk_met_areas,
        src_node,
        dst_node,
        SUM(weight) AS weight,
        COUNT(device_id) AS visitor_count
      FROM
        visits_once_per_device
      GROUP BY
        fk_met_areas,
        src_node,
        dst_node)
     WHERE visitor_count >= CAST("1" AS INT64) -- used to be 2 when observed (privacy)
  )
    
SELECT
  *
FROM
    visits_collect_all 