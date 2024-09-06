CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_monthly_table']}}` AS

SELECT
    name,
    fk_sgcenters,
    DATE_TRUNC(local_date, MONTH) AS local_date,
    SUM(agg_visits_tenants) AS agg_visits_tenants,
    SUM(agg_visits_tenants)/AVG(num_tenants) AS avg_visits_tenants,
    SUM(busiest_tenant_visits) AS busiest_tenant_visits,
    CAST(ROUND(AVG(num_tenants)) AS INT) AS num_tenants,
    CAST(
        ROUND(
            SUM(mall_visits)
        ) AS INT
    ) AS mall_visits
FROM
    `{{ var.value.env_project }}.{{ dag_run.conf['dataset_postgres_template'] }}.{{ params['visits_to_malls_daily_table'] }}` a
GROUP BY
    fk_sgcenters, name, local_date
ORDER BY 
    fk_sgcenters, local_date