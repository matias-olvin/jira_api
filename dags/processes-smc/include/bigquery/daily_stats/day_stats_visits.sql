DECLARE d DATE DEFAULT CAST('{{ params["date_start"] }}' as DATE);

WHILE d < '{{ params["date_end"] }}' DO
    IF(NOT EXISTS(
        SELECT 
            local_date 
        from 
            `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_table'] }}`
        WHERE 
            local_date = d
    )) THEN
    INSERT `{{ var.value.env_project }}.{{ params['smc_metrics_dataset'] }}.{{ params['day_stats_visits_table'] }}` 
    WITH
    poi_visits_table AS (
        SELECT
            `{{ var.value.env_project }}.{{ params['functions_dataset'] }}`.point_level2token(lat_long_visit_point, 8) AS s2_token,
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
            `{{ var.value.env_project }}.{{ params['smc_poi_visits_dataset'] }}.*`
        WHERE
            local_date = d
            AND visit_score.opening > 0 AND parent_bool = FALSE
    ),
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
        DATE(d) AS local_date,
        *
    FROM visits_table_all
    ;
    END IF;
  SET d = DATE_ADD(d, INTERVAL 1 DAY);
END WHILE;
