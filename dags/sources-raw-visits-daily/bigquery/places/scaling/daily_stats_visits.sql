-- INSERT `storage-prod-olvin-com.metrics.day_stats_visits_met_areas` (local_date,  fk_met_areas, publisher_id, part_of_day, d_devices_visits, n_visits, overlap_mean, n_visits_overlap_none, n_visits_overlap_low, n_visits_overlap_medium, n_visits_overlap_high)  -- PARTITION BY   CLUSTER BY fk_met_areas AS
    --CREATE OR REPLACE TABLE `storage-prod-olvin-com.metrics.day_stats_visits_met_areas` PARTITION BY   CLUSTER BY fk_met_areas, part_of_day, publisher_id AS
    WITH
    poi_visits_table AS (
    SELECT
        `{{ params['project'] }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 8) AS s2_token,
        -- TO_HEX(CAST(( -- We want the final result in hexadecimal
        -- SELECT
        -- STRING_AGG(
        --     CAST(S2_CELLIDFROMPOINT(lat_long_visit_point, 8) >> bit & 0x1 AS STRING), '' ORDER BY bit DESC) -- S2_CELLIDFROMPOINT returns an integer, convert to binary. 14 is the level of the cell
        --         FROM UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
        --         ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string, BYTES format is required to use TO_HEX
        --     )
        -- ) AS s2_token,
        IFNULL(publisher_id, 0) AS publisher_id,
        device_id,
        visit_score.opening AS visit_score,
        CASE
        WHEN local_hour < 6 OR local_hour > 22 THEN "night"
        WHEN local_hour > 6
        AND local_hour < 22 THEN "day"
        ELSE
        NULL
    END
        AS part_of_day
    FROM
        `{{ params['project'] }}.{{ params['poi_visits_dataset'] }}.*`
    WHERE
         local_date = "{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}"
        AND visit_score.opening > 0 AND parent_bool = FALSE),
    visits_table_all AS (
    SELECT
        s2_token,
        publisher_id,
        part_of_day,
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high
    FROM
        poi_visits_table
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
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high
    FROM
        poi_visits_table
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
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high
    FROM
        poi_visits_table
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
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high
    FROM
        poi_visits_table
    WHERE
        part_of_day IS NOT NULL
    GROUP BY
        part_of_day
    UNION ALL
    SELECT
        s2_token,
        publisher_id,
        NULL AS part_of_day,
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high
    FROM
        poi_visits_table
    GROUP BY
        s2_token,
        publisher_id
    UNION ALL
    SELECT
        NULL AS s2_token,
        publisher_id,
        NULL AS part_of_day,
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high
    FROM
        poi_visits_table
    GROUP BY
        publisher_id
    UNION ALL
    SELECT
        s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high
    FROM
        poi_visits_table
    GROUP BY
        s2_token
    UNION ALL
    SELECT
        NULL AS s2_token,
        NULL AS publisher_id,
        NULL AS part_of_day,
        COUNT(DISTINCT device_id) AS d_devices_visits,
        CAST(SUM(visit_score) AS INT64) AS n_visits,
        AVG(visit_score) AS overlap_mean,
        CAST(SUM(
        IF
            (visit_score > 0.75,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_none,
        CAST(SUM(
        IF
            (visit_score < 0.75
            AND visit_score >= 0.25,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_low,
        CAST(SUM(
        IF
            (visit_score < 0.25
            AND visit_score >= 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_medium,
        CAST(SUM(
        IF
            (visit_score < 0.05,
            visit_score,
            NULL)) AS INT64) AS n_visits_overlap_high,
    FROM
        poi_visits_table
    )
    SELECT
    DATE("{{ execution_date.subtract(days=var.value.latency_days_visits|int).strftime('%Y-%m-%d') }}") AS local_date,
    *
    FROM
    visits_table_all
