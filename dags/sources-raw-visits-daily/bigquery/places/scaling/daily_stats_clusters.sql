-- INSERT `storage-prod-olvin-com.metrics.day_stats_clusters` (local_date, s2_token, publisher_id, part_of_day, n_clusters, d_devices_clusters, n_clusters_0d, device_clusters_0d_mean, device_clusters_Nd_mean)  -- PARTITION BY local_date CLUSTER BY s2_token AS
    --CREATE OR REPLACE TABLE `storage-prod-olvin-com.metrics.day_stats_clusters` PARTITION BY local_date CLUSTER BY s2_token, part_of_day, publisher_id AS
    WITH
    device_visits_table AS (
    SELECT
        `{{ params['project'] }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 8) AS s2_token,
        -- TO_HEX(CAST(( -- We want the final result in hexadecimal
        --     SELECT
        --       STRING_AGG(
        --         CAST(S2_CELLIDFROMPOINT(lat_long_visit_point, 8) >> bit & 0x1 AS STRING), '' ORDER BY bit DESC) -- S2_CELLIDFROMPOINT returns an integer, convert to binary. 14 is the level of the cell
        --         FROM UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
        --     ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string, BYTES format is required to use TO_HEX
        --   )
        -- ) AS s2_token,
        IFNULL(publisher_id, 0) AS publisher_id,
        device_id,
        duration,
        CASE
        WHEN local_hour < 6 OR local_hour > 22 THEN "night"
        WHEN local_hour > 6
        AND local_hour < 22 THEN "day"
        ELSE
        NULL
    END
        AS part_of_day
    FROM
     `{{ params['project']  }}.{{ params['device_clusters_dataset'] }}.*` 
        -- `storage-prod-olvin-com.device_clusters.*`
    WHERE
    local_date = "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}" ),
    clusters_table_all AS (
    SELECT
        s2_token,
        publisher_id,
        part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table
    GROUP BY
        s2_token,
        publisher_id
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table
    GROUP BY
        publisher_id
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table
    GROUP BY
        s2_token
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        COUNT(*) AS n_clusters,
        COUNT(DISTINCT device_id) AS d_devices_clusters
    FROM
        device_visits_table ),
    device_visits_table_0d AS (
    SELECT
        *
    FROM
        device_visits_table
    WHERE
        duration = 0
    ),
    clusters_table_0d AS (
    SELECT
        s2_token,
        publisher_id,
        part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        s2_token,
        publisher_id,
        part_of_day,
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        s2_token,
        publisher_id,
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        publisher_id,
        part_of_day,
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        publisher_id,
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        s2_token,
        part_of_day,
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        s2_token,
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        part_of_day,
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        publisher_id,
        NULL AS part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        s2_token,
        publisher_id,
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        s2_token,
        publisher_id,
        device_id )
    GROUP BY
        s2_token,
        publisher_id
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        NULL AS part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        publisher_id,
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        publisher_id,
        device_id )
    GROUP BY
        publisher_id
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        s2_token,
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        s2_token,
        device_id )
    GROUP BY
        s2_token
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        SUM(device_clusters_0d) AS n_clusters_0d,
        AVG(device_clusters_0d) AS device_clusters_0d_mean
    FROM (
        SELECT
        COUNT(*) AS device_clusters_0d
        FROM
        device_visits_table_0d
        GROUP BY
        device_id )),
    device_visits_table_Nd AS (
    SELECT
        *
    FROM
        device_visits_table
    WHERE
        duration <> 0 ),
    clusters_table_Nd AS (
    SELECT
        s2_token,
        publisher_id,
        part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        s2_token,
        publisher_id,
        part_of_day,
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        s2_token,
        publisher_id,
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        publisher_id,
        part_of_day,
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        publisher_id,
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        publisher_id,
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        s2_token,
        part_of_day,
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        s2_token,
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        s2_token,
        part_of_day
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        part_of_day,
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        part_of_day,
        device_id )
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        publisher_id,
        NULL AS part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        s2_token,
        publisher_id,
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        s2_token,
        publisher_id,
        device_id )
    GROUP BY
        s2_token,
        publisher_id
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        NULL AS part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        publisher_id,
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        publisher_id,
        device_id )
    GROUP BY
        publisher_id
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        s2_token,
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        s2_token,
        device_id )
    GROUP BY
        s2_token
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        AVG(device_clusters_Nd) AS device_clusters_Nd_mean
    FROM (
        SELECT
        COUNT(*) AS device_clusters_Nd
        FROM
        device_visits_table_Nd
        GROUP BY
        device_id )),
    final_table AS (
    SELECT
        COALESCE(clusters_table_all.s2_token,
        clusters_table_0d.s2_token,
        clusters_table_Nd.s2_token) AS s2_token,
        COALESCE(clusters_table_all.publisher_id,
        clusters_table_0d.publisher_id,
        clusters_table_Nd.publisher_id) AS publisher_id,
        COALESCE(clusters_table_all.part_of_day,
        clusters_table_0d.part_of_day,
        clusters_table_Nd.part_of_day) AS part_of_day,
        clusters_table_all.* EXCEPT(
        s2_token,
        publisher_id,
        part_of_day ),
        clusters_table_0d.* EXCEPT(
        s2_token,
        publisher_id,
        part_of_day ),
        clusters_table_Nd.* EXCEPT(
        s2_token,
        publisher_id,
        part_of_day ),
    FROM
        clusters_table_all
    FULL OUTER JOIN
        clusters_table_0d
    ON
         ( clusters_table_all.s2_token = clusters_table_0d.s2_token
        OR ( clusters_table_all.s2_token IS NULL
            AND clusters_table_0d.s2_token IS NULL ) )
        AND ( clusters_table_all.publisher_id = clusters_table_0d.publisher_id
        OR ( clusters_table_all.publisher_id IS NULL
            AND clusters_table_0d.publisher_id IS NULL ) )
        AND ( clusters_table_all.part_of_day = clusters_table_0d.part_of_day
        OR ( clusters_table_all.part_of_day IS NULL
            AND clusters_table_0d.part_of_day IS NULL ) )
    FULL OUTER JOIN
        clusters_table_Nd
    ON
        ( clusters_table_all.s2_token = clusters_table_Nd.s2_token
        OR ( clusters_table_all.s2_token IS NULL
            AND clusters_table_Nd.s2_token IS NULL ) )
        AND ( clusters_table_all.publisher_id = clusters_table_Nd.publisher_id
        OR ( clusters_table_all.publisher_id IS NULL
            AND clusters_table_Nd.publisher_id IS NULL ) )
        AND ( clusters_table_all.part_of_day = clusters_table_Nd.part_of_day
        OR ( clusters_table_all.part_of_day IS NULL
            AND clusters_table_Nd.part_of_day IS NULL ) )
     )
    SELECT
    DATE("{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}") AS local_date,
    *,
    FROM
    final_table