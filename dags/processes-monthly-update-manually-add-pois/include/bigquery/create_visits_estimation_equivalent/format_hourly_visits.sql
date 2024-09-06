CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{  params['sgplace_hourly_visits_raw_table'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgplaces AS
SELECT
    *
FROM
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{  params['sgplace_hourly_all_visits_raw_table'] }}`
WHERE
    local_date >= DATE_TRUNC(
        DATE_SUB(CAST("{{ ds }}" AS DATE), INTERVAL 0 MONTH),
        MONTH
    )
    AND local_date <= DATE_ADD(CAST("{{ ds }}" AS DATE), INTERVAL 5 MONTH);