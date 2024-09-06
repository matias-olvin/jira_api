CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['median_table'] }}_{{ stats_group }}` AS
SELECT
    statistic,
    cum_percentage,
    percentage
FROM (
    SELECT
        statistic,
        ROW_NUMBER() OVER(ORDER BY cum_percentage, percentage) AS row_n,
        cum_percentage,
        percentage
    FROM (
        SELECT
            statistic,
            SUM(percentage) OVER (ORDER BY statistic) AS cum_percentage,
            percentage,
        FROM
            `{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_demographics_table'] }}_display`
        WHERE stats_group = "{{ stats_group }}"
    )
    WHERE cum_percentage > 50
)
WHERE row_n = 1