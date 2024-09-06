WITH 
    today AS (
        SELECT
            CAST(IFNULL(SUM(n_visits), 0) AS NUMERIC) AS n_visits,
            CAST(IFNULL(SUM(d_devices_visits), 0) AS NUMERIC) AS d_devices_visits,
            CAST(IFNULL(SUM(n_visits_overlap_none), 0) AS NUMERIC) AS n_visits_overlap_none,
            CAST(IFNULL(SUM(n_visits_overlap_low), 0) AS NUMERIC) AS n_visits_overlap_low,
            CAST(IFNULL(SUM(n_visits_overlap_medium), 0) AS NUMERIC) AS n_visits_overlap_medium,
            CAST(IFNULL(SUM(n_visits_overlap_high), 0) AS NUMERIC) AS n_visits_overlap_high,
            CAST(IFNULL(AVG(overlap_mean), 0) AS NUMERIC) AS overlap_mean
        FROM 
            `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`
        WHERE
            local_date = DATE_SUB(DATE('{{ ds }}'), INTERVAL {{ var.value.latency_days_visits }} DAY)
    ),
    yesterday AS (
        SELECT
            CAST(IFNULL(SUM(n_visits), 0) AS NUMERIC) AS n_visits,
            CAST(IFNULL(SUM(d_devices_visits), 0) AS NUMERIC) AS d_devices_visits,
            CAST(IFNULL(SUM(n_visits_overlap_none), 0) AS NUMERIC) AS n_visits_overlap_none,
            CAST(IFNULL(SUM(n_visits_overlap_low), 0) AS NUMERIC) AS n_visits_overlap_low,
            CAST(IFNULL(SUM(n_visits_overlap_medium), 0) AS NUMERIC) AS n_visits_overlap_medium,
            CAST(IFNULL(SUM(n_visits_overlap_high), 0) AS NUMERIC) AS n_visits_overlap_high,
            CAST(IFNULL(AVG(overlap_mean), 0) AS NUMERIC) AS overlap_mean
        FROM 
            `{{ params['project'] }}.{{ params['metrics_dataset'] }}.{{ params['day_stats_visits_scaled_table'] }}`
        WHERE
            local_date = DATE_SUB(DATE_SUB(DATE('{{ ds }}'), INTERVAL {{ var.value.latency_days_visits }} DAY), INTERVAL 1 DAY)
    ),
    difference AS (
        SELECT
            IFNULL(SAFE_DIVIDE(today.n_visits, yesterday.n_visits), 0) AS n_visits,
            IFNULL(SAFE_DIVIDE(today.d_devices_visits, yesterday.d_devices_visits), 0) AS d_devices_visits,
            IFNULL(SAFE_DIVIDE(today.n_visits_overlap_none, yesterday.n_visits_overlap_none), 0) AS n_visits_overlap_none,
            IFNULL(SAFE_DIVIDE(today.n_visits_overlap_low, yesterday.n_visits_overlap_low), 0) AS n_visits_overlap_low,
            IFNULL(SAFE_DIVIDE(today.n_visits_overlap_medium, yesterday.n_visits_overlap_medium), 0) AS n_visits_overlap_medium,
            IFNULL(SAFE_DIVIDE(today.n_visits_overlap_high, yesterday.n_visits_overlap_high), 0) AS n_visits_overlap_high,
            IFNULL(SAFE_DIVIDE(today.overlap_mean, yesterday.overlap_mean), 0) AS overlap_mean
        FROM 
            today, 
            yesterday
    )

SELECT * 
FROM difference