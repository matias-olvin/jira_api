IF "{{ dag_run.conf['devices'] }}" = "visitors" THEN
    CREATE OR REPLACE TABLE
      `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`
    PARTITION BY local_date AS
    SELECT
        visits_table.device_id,
        visits_table.lat_long_visit_point,
        visits_table.duration,
        visits_table.local_date,
        visits_table.local_hour,
        visits_table.visit_score,
            MAX(visits_table.local_date) OVER (PARTITION BY visits_table.device_id) AS max_local_date,
            MIN(visits_table.local_date) OVER (PARTITION BY visits_table.device_id) AS min_local_date,
      homes_table.home_point,
      homes_table.work_point
    FROM
     `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_table'] }}`
      AS visits_table
    JOIN (
      SELECT
        *
      FROM
        `{{ var.value.storage_project_id }}.{{ params['device_homes_dataset'] }}.*`
      WHERE
        local_date >= "{{ dag_run.conf['start_date_string'] }}" AND local_date < "{{ dag_run.conf['end_date_string'] }}") AS homes_table
    ON
      visits_table.device_id = homes_table.device_id
      AND DATE_TRUNC(visits_table.local_date, MONTH) = homes_table.local_date;
ELSEIF "{{ dag_run.conf['devices'] }}" = "workers" THEN
    CREATE OR REPLACE TABLE
    `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`
    PARTITION BY local_date AS
    WITH target_geog AS (
    SELECT
        ST_BUFFER(ST_UNION_AGG(lat_long_visit_point), 30) AS polygon_site,
      FROM
        `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_table'] }}`
    ),
    id_lookup AS (
      SELECT
        device_id,
        lat_long_visit_point,
        SUM(duration) OVER (PARTITION BY device_id) AS duration,
        DATE_TRUNC(local_date, MONTH) AS local_date_org,
      FROM
        `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['raw_visits_table'] }}`
    ),
    id_dates AS (
      SELECT
        local_date,
        device_id,
        max_local_date,
        min_local_date,
        lat_long_visit_point,
        duration,
      FROM
        (
          SELECT
            local_date,
            device_id,
            ROW_NUMBER() OVER (
              PARTITION BY device_id
              ORDER BY
                local_date DESC
            ) AS rank_date,
            MAX(local_date) OVER (PARTITION BY device_id) AS max_local_date,
            MIN(local_date) OVER (PARTITION BY device_id) AS min_local_date,
            lat_long_visit_point,
            duration,
          FROM
            (
              SELECT
                device_id,
                local_date,
                ST_CENTROID(ST_UNION_AGG(lat_long_visit_point)) AS lat_long_visit_point,
                ANY_VALUE(duration) AS duration,
              FROM
                (
                  SELECT
                    device_id,
                    local_date_org,
                    ST_CENTROID(ST_UNION_AGG(lat_long_visit_point)) AS lat_long_visit_point,
                    ANY_VALUE(duration) AS duration,
                  FROM
                    id_lookup
                  GROUP BY
                    device_id,
                    local_date_org
                )
                CROSS JOIN UNNEST(
                  GENERATE_DATE_ARRAY(
                    local_date_org,
                    DATE_ADD(local_date_org, INTERVAL 1 MONTH),
                    INTERVAL 1 MONTH
                  )
                ) AS local_date
              GROUP BY
                local_date,
                device_id
              HAVING
                COUNT(*) >= 2
            )
        )
      WHERE
        rank_date = 1
    )
    SELECT
        device_id AS device_id,
        lat_long_visit_point,
        duration,
        local_date,
        -1 AS local_hour,
        1 AS visit_score,
        max_local_date,
        min_local_date,
      estimated_home AS home_point,
      CAST(NULL AS GEOGRAPHY) AS work_point
    FROM
      (
        SELECT
          id_dates.device_id,
          id_dates.local_date,
          max_local_date,
          min_local_date,
          IFNULL(home_point, work_point) AS estimated_home,
        lat_long_visit_point,
        duration,
          ROW_NUMBER() OVER (
            PARTITION BY id_dates.device_id
            ORDER BY
              id_dates.local_date DESC
          ) AS rank_date,
        FROM
          id_dates
          JOIN(
            SELECT
              device_id,
              local_date,
              IF(
                ST_CONTAINS(polygon_site, home_point),
                NULL,
                home_point
              ) AS home_point,
              IF(
                ST_CONTAINS(polygon_site, work_point),
                NULL,
                work_point
              ) AS work_point
            FROM
              (SELECT
              device_id, local_date, home_point, work_point FROM
              `{{ var.value.storage_project_id }}.{{ params['device_homes_dataset'] }}.*`
              WHERE
                local_date >= "{{ dag_run.conf['start_date_string'] }}" AND local_date < "{{ dag_run.conf['end_date_string'] }}"
                AND
              home_point IS NOT NULL
              OR work_point IS NOT NULL
                )
              CROSS JOIN target_geog
          ) AS homes_table ON homes_table.device_id = id_dates.device_id
          AND homes_table.local_date <= id_dates.local_date
          AND (
            home_point IS NOT NULL
            OR work_point IS NOT NULL
          )
      )
    WHERE
      rank_date = 1;
ELSE
    RAISE USING MESSAGE = "Value for dag_run.conf['devices'] must be 'visitors' or 'workers', not '{{ dag_run.conf['devices'] }}'.";
END IF;