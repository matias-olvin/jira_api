CREATE OR REPLACE VIEW
    `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['kpi_v2_table_chart_view'] }}` AS
WITH
    avg_yoy_error AS (
        SELECT
            id,
            run_date,
            AVG(yoy[SAFE_OFFSET(0)].yoy_error) AS yoy_error,
            AVG(yoy[SAFE_OFFSET(0)].yoy_error_abs) AS yoy_error_abs
        FROM
            `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['trend_metrics_table'] }}` trend
        WHERE
            trend.step LIKE "raw"
        GROUP BY
            id,
            run_date
        ORDER BY
            id,
            run_date
    ),
    trend_place AS (
        SELECT DISTINCT
            trend.id,
            place.fk_sgbrands,
            place.name,
            place.street_address,
            place.top_category,
            place.naics_code,
            place.region,
            t_corr.run_date,
            t_corr.median_correlation_daily,
            t_corr.median_correlation_weekly,
            t_corr.median_correlation_monthly,
            trend.yoy_error,
            trend.yoy_error_abs
        FROM
            avg_yoy_error trend
            JOIN `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['dwm_correlations_table'] }}_latest_mu` t_corr ON trend.id = t_corr.id
            AND trend.run_date = t_corr.run_date
            LEFT JOIN `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` place ON trend.id = place.pid
    ),
    vol_table AS (
        SELECT
            id,
            run_date,
            AVG(kendall) AS kendall,
            AVG(divergence) AS divergence
        FROM
            `{{ var.value.env_project }}.{{ params['postgres_metrics_dataset'] }}.{{ params['volume_metrics_table'] }}` v
        WHERE
            v.class_id LIKE "fk_sgbrands"
            AND v.step LIKE "raw"
        GROUP BY
            id,
            run_date
        ORDER BY
            id,
            run_date
    )
SELECT
    tp.*,
    v.divergence,
    v.kendall
FROM
    trend_place tp
    LEFT JOIN vol_table v ON v.id = tp.fk_sgbrands
    AND v.run_date = tp.run_date;