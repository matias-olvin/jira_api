CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplace_monthly_visits_raw'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgplaces AS
WITH
    monthly_adjustments_output AS (
        SELECT
            fk_sgplaces,
            DATE_TRUNC(local_date, MONTH) AS local_date,
            visits
        FROM
            `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['daily_visits_table'] }}`
    ),
    monthly_visits_table AS (
        SELECT
            fk_sgplaces,
            local_date,
            SUM(visits) AS visits
        FROM
            monthly_adjustments_output
        GROUP BY
            fk_sgplaces,
            local_date
    )
SELECT
    *
FROM
    monthly_visits_table