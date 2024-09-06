CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['dwm_correlations_table'] }}_latest_mu` AS
SELECT
    *
FROM
    `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['dwm_correlations_table'] }}`
WHERE
    run_date = (
        SELECT
            MAX(run_date)
        FROM
            `{{ var.value.env_project }}.{{ params['validations_kpi_dataset'] }}.{{ params['dwm_correlations_table'] }}`
    );