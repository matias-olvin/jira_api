CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['dwm_correlations_table'] }}`
PARTITION BY
    run_date
CLUSTER BY
    id AS
WITH
    all_time AS (
        SELECT -- duration all time
            DISTINCT id,
            run_date,
            granularity,
            "All Time" AS duration,
            NULL AS median_correlation_daily,
            NULL AS median_correlation_weekly,
            PERCENTILE_CONT(poi_correlation, 0.5) OVER (
                PARTITION BY
                    id,
                    run_date
            ) AS median_correlation_monthly
        FROM
            (
                SELECT
                    id,
                    run_date,
                    granularity,
                    corr_element.poi_correlation
                FROM
                    `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}`,
                    UNNEST (correlation) AS corr_element
                WHERE
                    class_id = 'fk_sgbrands'
                    AND correlation[SAFE_OFFSET(0)].days = 1642
                    AND correlation[SAFE_OFFSET(1)].days IS NULL
                    AND granularity = 'month'
                    AND pipeline = 'postgres'
                    AND step = 'raw'
            )
    ),
    one_year AS (
        SELECT -- duration one year
            DISTINCT id,
            run_date,
            granularity,
            "1 Year" AS duration,
            NULL AS median_correlation_daily,
            NULL AS median_correlation_weekly,
            PERCENTILE_CONT(poi_correlation, 0.5) OVER (
                PARTITION BY
                    id,
                    run_date
            ) AS median_correlation_monthly
        FROM
            (
                SELECT
                    id,
                    run_date,
                    granularity,
                    corr_element.poi_correlation
                FROM
                    `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}`,
                    UNNEST (correlation) AS corr_element
                WHERE
                    class_id = 'fk_sgbrands'
                    AND correlation[SAFE_OFFSET(0)].days = 1642
                    AND correlation[SAFE_OFFSET(1)].days = 365
                    AND granularity = 'month'
                    AND pipeline = 'postgres'
                    AND step = 'raw'
            )
    ),
    weekly AS (
        SELECT -- duration 6 months
            DISTINCT id,
            run_date,
            granularity,
            "6 Month" AS duration,
            NULL AS median_correlation_daily,
            PERCENTILE_CONT(poi_correlation, 0.5) OVER (
                PARTITION BY
                    id,
                    run_date
            ) AS median_correlation_weekly,
            NULL AS median_correlation_monthly
        FROM
            (
                SELECT
                    id,
                    run_date,
                    granularity,
                    corr_element.poi_correlation
                FROM
                    `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}`,
                    UNNEST (correlation) AS corr_element
                WHERE
                    class_id = 'fk_sgbrands'
                    AND correlation[SAFE_OFFSET(0)].days = 182
                    AND granularity = 'week'
                    AND pipeline = 'postgres'
                    AND step = 'raw'
            )
    ),
    daily AS (
        SELECT -- duration 3 Months
            DISTINCT id,
            run_date,
            granularity,
            "3 Month" AS duration,
            PERCENTILE_CONT(poi_correlation, 0.5) OVER (
                PARTITION BY
                    id,
                    run_date
            ) AS median_correlation_daily,
            NULL AS median_correlation_weekly,
            NULL AS median_correlation_monthly
        FROM
            (
                SELECT
                    id,
                    run_date,
                    granularity,
                    corr_element.poi_correlation
                FROM
                    `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}`,
                    UNNEST (correlation) AS corr_element
                WHERE
                    class_id = 'fk_sgbrands'
                    AND correlation[SAFE_OFFSET(0)].days = 91
                    AND granularity = 'day'
                    AND pipeline = 'postgres'
                    AND step = 'raw'
            )
    ),
    combined_table AS (
        SELECT
            *
        FROM
            all_time
        UNION ALL
        SELECT
            *
        FROM
            one_year
        UNION ALL
        SELECT
            *
        FROM
            weekly
        UNION ALL
        SELECT
            *
        FROM
            daily
    )
SELECT
    ct.id,
    ct.run_date,
    ct.granularity,
    ct.duration,
    IFNULL(
        ct.median_correlation_daily,
        d.median_correlation_daily
    ) AS median_correlation_daily,
    IFNULL(
        ct.median_correlation_weekly,
        w.median_correlation_weekly
    ) AS median_correlation_weekly,
    ct.median_correlation_monthly
FROM
    combined_table ct
    LEFT JOIN daily d ON ct.id = d.id
    AND ct.run_date = d.run_date
    LEFT JOIN weekly w ON ct.id = w.id
    AND ct.run_date = w.run_date;